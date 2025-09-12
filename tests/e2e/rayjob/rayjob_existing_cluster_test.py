import pytest
import sys
import os
from time import sleep

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from support import *

from codeflare_sdk import (
    Cluster,
    ClusterConfiguration,
)
from codeflare_sdk.ray.rayjobs import RayJob
from codeflare_sdk.ray.rayjobs.status import CodeflareRayJobStatus
from python_client.kuberay_job_api import RayjobApi


@pytest.mark.openshift
class TestRayJobExistingCluster:
    """Test RayJob against existing clusters (Kueue and non-Kueue)."""

    def setup_method(self):
        initialize_kubernetes_client(self)

    def teardown_method(self):
        delete_namespace(self)
        delete_kueue_resources(self)

    def test_existing_kueue_cluster(self):
        """Test RayJob against Kueue-managed RayCluster."""
        self.setup_method()
        create_namespace(self)
        create_kueue_resources(self)

        cluster_name = "kueue-cluster"

        cluster = Cluster(
            ClusterConfiguration(
                name=cluster_name,
                namespace=self.namespace,
                num_workers=1,
                head_cpu_requests="500m",
                head_cpu_limits="500m",
                worker_cpu_requests=1,
                worker_cpu_limits=1,
                worker_memory_requests=1,
                worker_memory_limits=4,
                image=get_ray_image(),
                local_queue=self.local_queues[0],  # Kueue-managed cluster
                write_to_file=True,
                verify_tls=False,
            )
        )

        cluster.apply()
        cluster.wait_ready()

        # RayJob against Kueue-managed cluster with its own local_queue
        rayjob_kueue = RayJob(
            job_name="job-kueue",
            cluster_name=cluster_name,
            namespace=self.namespace,
            entrypoint="python -c \"import ray; ray.init(); print('Job on Kueue cluster')\"",
            runtime_env={"env_vars": get_setup_env_variables(ACCELERATOR="cpu")},
            local_queue=self.local_queues[0],
        )

        # RayJob against Kueue-managed cluster without local_queue
        rayjob_non_kueue = RayJob(
            job_name="job-non-kueue",
            cluster_name=cluster_name,
            namespace=self.namespace,
            entrypoint="python -c \"import ray; ray.init(); print('Non-Kueue job')\"",
            runtime_env={"env_vars": get_setup_env_variables(ACCELERATOR="cpu")},
        )

        try:
            # Test Kueue-managed RayJob
            assert rayjob_kueue.submit() == "job-kueue"
            assert rayjob_kueue.stop() is False  # Cannot suspend Kueue jobs
            self._wait_completion(rayjob_kueue)

            # Test non-Kueue RayJob
            assert rayjob_non_kueue.submit() == "job-non-kueue"
            assert rayjob_non_kueue.stop() is True  # Can suspend non-Kueue jobs
            assert rayjob_non_kueue.resubmit() is True
            self._wait_completion(rayjob_non_kueue)
        finally:
            rayjob_kueue.delete()
            rayjob_non_kueue.delete()
            cluster.down()

    def test_existing_non_kueue_cluster(self):
        """Test RayJob against non-Kueue RayCluster."""
        self.setup_method()
        create_namespace(self)

        cluster_name = "standard-cluster"

        cluster = Cluster(
            ClusterConfiguration(
                name=cluster_name,
                namespace=self.namespace,
                num_workers=1,
                head_cpu_requests="500m",
                head_cpu_limits="500m",
                worker_cpu_requests=1,
                worker_cpu_limits=1,
                worker_memory_requests=1,
                worker_memory_limits=4,
                image=get_ray_image(),
                # No local_queue - not Kueue managed
                write_to_file=True,
                verify_tls=False,
            )
        )

        cluster.apply()
        cluster.wait_ready()

        rayjob = RayJob(
            job_name="standard-job",
            cluster_name=cluster_name,
            namespace=self.namespace,
            entrypoint="python -c \"import ray; ray.init(); print('Standard job done')\"",
            runtime_env={"env_vars": get_setup_env_variables(ACCELERATOR="cpu")},
        )

        try:
            assert rayjob.submit() == "standard-job"

            # Test stop/resubmit for non-Kueue job
            job_api = RayjobApi()
            assert job_api.wait_until_job_running(
                name=rayjob.name, k8s_namespace=rayjob.namespace, timeout=300
            )

            assert rayjob.stop() is True
            assert wait_for_job_status(
                job_api, rayjob.name, rayjob.namespace, "Suspended", timeout=30
            )

            assert rayjob.resubmit() is True
            self._wait_completion(rayjob)
        finally:
            rayjob.delete()
            cluster.down()

    def _wait_completion(self, rayjob: RayJob, timeout: int = 600):
        """Wait for RayJob completion."""
        elapsed = 0
        interval = 10

        while elapsed < timeout:
            status, _ = rayjob.status(print_to_console=False)
            if status == CodeflareRayJobStatus.COMPLETE:
                return
            elif status == CodeflareRayJobStatus.FAILED:
                raise AssertionError(f"RayJob '{rayjob.name}' failed")
            sleep(interval)
            elapsed += interval

        raise TimeoutError(f"RayJob '{rayjob.name}' timeout after {timeout}s")
