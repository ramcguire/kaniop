use serial_test::serial;

use super::{
    DEFAULT_REPLICA_GROUP_NAME, STORAGE_VOLUME_CLAIM_TEMPLATE_JSON, is_kanidm, is_kanidm_false,
    is_statefulset_ready, pod_version_label, setup, statefulset_version_label, wait_for,
    wait_for_replication_success_with_timeout,
};
use crate::kanidm::get_dependency_version;
use crate::test::poll_until;

use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use json_patch::merge;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, Patch, PatchParams};
use serde::Deserialize;
use serde_json::json;
use tokio::time::{Instant, sleep};

#[derive(Deserialize)]
struct CratesVersionsResponse {
    versions: Vec<CrateVersion>,
}

#[derive(Deserialize)]
struct CrateVersion {
    num: String,
    yanked: bool,
}

fn parse_semver(version: &str) -> Option<(u64, u64, u64)> {
    let parts: Vec<&str> = version.split('.').collect();
    if parts.len() >= 3 {
        let major = parts[0].parse().ok()?;
        let minor = parts[1].parse().ok()?;
        let patch = parts[2].split('-').next()?.parse().ok()?;
        Some((major, minor, patch))
    } else {
        None
    }
}

fn fetch_previous_minor_from_crates_io(current_major: u64, current_minor: u64) -> Option<String> {
    let response: CratesVersionsResponse =
        ureq::get("https://crates.io/api/v1/crates/kanidm_client/versions?per_page=100")
            .header("User-Agent", "kaniop-e2e-test")
            .call()
            .ok()?
            .body_mut()
            .read_json()
            .ok()?;

    let mut best: Option<(u64, u64, u64)> = None;
    for v in &response.versions {
        if v.yanked {
            continue;
        }
        let Some((major, minor, patch)) = parse_semver(&v.num) else {
            continue;
        };
        if major == current_major && minor == current_minor {
            continue;
        }
        let is_previous_minor = if current_minor > 0 {
            major == current_major && minor == current_minor - 1
        } else {
            major == current_major - 1
        };
        if !is_previous_minor {
            continue;
        }
        if best.is_none_or(|(b_major, b_minor, b_patch)| {
            (major, minor, patch) > (b_major, b_minor, b_patch)
        }) {
            best = Some((major, minor, patch));
        }
    }
    best.map(|(major, minor, patch)| format!("{major}.{minor}.{patch}"))
}

fn previous_minor_version() -> String {
    let current = get_dependency_version().unwrap();
    let (current_major, current_minor, _) = parse_semver(&current).unwrap();

    if current_minor > 0 {
        format!("{current_major}.{}.0", current_minor - 1)
    } else {
        fetch_previous_minor_from_crates_io(current_major, current_minor)
            .expect("Failed to determine previous minor version from crates.io")
    }
}

fn get_statefulset_image(sts: &StatefulSet) -> String {
    sts.spec
        .as_ref()
        .unwrap()
        .template
        .spec
        .as_ref()
        .unwrap()
        .containers
        .first()
        .unwrap()
        .image
        .clone()
        .unwrap()
}

e2e_test!(
    #[serial(replication)]
    kanidm_upgrade_ha_cluster,
    {
        let name = "test-upgrade-ha-cluster";
        let prev_version = previous_minor_version();
        let current_version = get_dependency_version().unwrap();
        let prev_image = format!("kanidm/server:{prev_version}");
        let current_image = format!("kanidm/server:{current_version}");

        let mut spec_patch = json!({
            "image": prev_image,
            "replicaGroups": [{"name": DEFAULT_REPLICA_GROUP_NAME, "replicas": 2, "primaryNode": true}],
        });
        merge(&mut spec_patch, &STORAGE_VOLUME_CLAIM_TEMPLATE_JSON.clone());

        let s = setup(name, Some(spec_patch)).await;

        let sts_name = format!("{name}-{DEFAULT_REPLICA_GROUP_NAME}");
        let sts = s.statefulset_api.get(&sts_name).await.unwrap();
        assert_eq!(sts.spec.as_ref().unwrap().replicas.unwrap(), 2);
        assert_eq!(get_statefulset_image(&sts), prev_image);

        wait_for(s.kanidm_api.clone(), name, is_kanidm("Available")).await;
        wait_for(s.kanidm_api.clone(), name, is_kanidm("Initialized")).await;
        wait_for(s.kanidm_api.clone(), name, is_kanidm_false("Progressing")).await;

        let pod_api = Api::<Pod>::namespaced(s.client.clone(), "default");
        let pod_names = (0..2)
            .map(|i| format!("{sts_name}-{i}"))
            .collect::<Vec<_>>();
        wait_for_replication_success_with_timeout(&pod_api, &pod_names).await;

        // Baseline before the rollout starts: the version label should already be set from the
        // initial (non-rolling) reconcile, giving us a known-good value to detect regressions
        // (the label flipping to absent mid-rollout) against below.
        let statefulset_api = s.statefulset_api.clone();
        let pre_upgrade_version_label: String =
            poll_until("StatefulSet version label set before upgrade", || async {
                let sts = statefulset_api.get(&sts_name).await.ok()?;
                statefulset_version_label(&sts)
            })
            .await;

        let kanidm_api = s.kanidm_api.clone();
        let retryable_patch = || async {
            let kanidm = kanidm_api.get(name).await?;
            let mut patch_kanidm = kanidm.clone();
            patch_kanidm.spec.image = current_image.clone();
            patch_kanidm.metadata.managed_fields = None;
            kanidm_api
                .patch(
                    name,
                    &PatchParams::apply("e2e-test").force(),
                    &Patch::Apply(&patch_kanidm),
                )
                .await
        };
        retryable_patch
            .retry(ExponentialBuilder::default().with_max_times(5))
            .await
            .unwrap();

        // Regression test for the sticky, settled-rollout-gated StatefulSet version label: it
        // must never go absent mid-rollout (a single pod not yet reporting its version used to
        // wipe the label via SSA every reconcile) — it should only ever hold the pre-upgrade
        // value until the rollout settles, then jump straight to the post-upgrade value.
        let rollout_start = Instant::now();
        loop {
            assert!(
                rollout_start.elapsed() < Duration::from_secs(120),
                "Timed out waiting for StatefulSet image to update to {current_image}"
            );
            // Tolerate transient API server errors here (as poll_until does elsewhere in this
            // suite) instead of failing the whole test on a single hiccup during the rollout.
            let Ok(sts) = statefulset_api.get(&sts_name).await else {
                sleep(Duration::from_secs(2)).await;
                continue;
            };
            let image = get_statefulset_image(&sts);
            let label = statefulset_version_label(&sts);

            assert!(
                label.is_some(),
                "StatefulSet version label disappeared mid-rollout (was {pre_upgrade_version_label})"
            );

            if image == current_image {
                break;
            }
            sleep(Duration::from_secs(2)).await;
        }

        wait_for(s.kanidm_api.clone(), name, is_kanidm("Available")).await;
        wait_for(s.kanidm_api.clone(), name, is_kanidm_false("Progressing")).await;
        wait_for(s.statefulset_api.clone(), &sts_name, is_statefulset_ready).await;

        let upgraded_sts = s.statefulset_api.get(&sts_name).await.unwrap();
        assert_eq!(get_statefulset_image(&upgraded_sts), current_image);
        assert_eq!(upgraded_sts.spec.as_ref().unwrap().replicas.unwrap(), 2);

        wait_for_replication_success_with_timeout(&pod_api, &pod_names).await;

        // Once settled, the label must advance past the pre-upgrade value (not just stay
        // stuck), and every pod's own label must agree with the StatefulSet's.
        let final_version_label: String = poll_until(
            "StatefulSet version label settled to post-upgrade version",
            || async {
                let sts = statefulset_api.get(&sts_name).await.ok()?;
                let label = statefulset_version_label(&sts)?;
                (label != pre_upgrade_version_label).then_some(label)
            },
        )
        .await;

        for pod_name in &pod_names {
            poll_until(
                "Pod version label matches settled StatefulSet version label",
                || async {
                    let pod = pod_api.get(pod_name).await.ok()?;
                    (pod_version_label(&pod).as_ref() == Some(&final_version_label)).then_some(())
                },
            )
            .await;
        }
    }
);
