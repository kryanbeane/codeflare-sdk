import pytest
import sys
import os
from time import sleep

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from support import *

from codeflare_sdk import RayJob, ManagedClusterConfig
import kubernetes.client.rest
from python_client.kuberay_job_api import RayjobApi
from python_client.kuberay_cluster_api import RayClusterApi


@pytest.mark.openshift
class TestRayJobLifecycledCluster:
    """Test RayJob with auto-created cluster lifecycle management."""

    def setup_method(self):
        initialize_kubernetes_client(self)

    def teardown_method(self):
        delete_namespace(self)
        delete_kueue_resources(self)

    def test_lifecycled_kueue_managed(self):
        """Test RayJob with Kueue-managed lifecycled cluster."""
        self.setup_method()
        create_namespace(self)
        create_kueue_resources(self)

        self.job_api = RayjobApi()
        cluster_api = RayClusterApi()
        job_name = "kueue-lifecycled"

        cluster_config = ManagedClusterConfig(
            head_cpu_requests="500m",
            head_cpu_limits="500m",
            head_memory_requests=2,
            head_memory_limits=3,
            num_workers=1,
            worker_cpu_requests="500m",
            worker_cpu_limits="500m",
            worker_memory_requests=1,
            worker_memory_limits=2,
            image=get_ray_image(),
        )

        rayjob = RayJob(
            job_name=job_name,
            namespace=self.namespace,
            cluster_config=cluster_config,
            entrypoint="python -c \"import ray; ray.init(); print('Kueue job done')\"",
            runtime_env={"env_vars": get_setup_env_variables(ACCELERATOR="cpu")},
            local_queue=self.local_queues[0],
        )

        try:
            assert rayjob.submit() == job_name
            assert self.job_api.wait_until_job_running(
                name=rayjob.name, k8s_namespace=rayjob.namespace, timeout=600
            )

            # Kueue-managed jobs cannot be suspended
            assert rayjob.stop() is False

            assert self.job_api.wait_until_job_finished(
                name=rayjob.name, k8s_namespace=rayjob.namespace, timeout=300
            )
        finally:
            assert rayjob.delete()
            verify_rayjob_cluster_cleanup(cluster_api, rayjob.name, rayjob.namespace)

    def test_lifecycled_non_kueue(self):
        """Test RayJob with non-Kueue lifecycled cluster (stop/resubmit)."""
        self.setup_method()
        create_namespace(self)

        self.job_api = RayjobApi()
        cluster_api = RayClusterApi()
        job_name = "non-kueue-lifecycled"

        cluster_config = ManagedClusterConfig(
            head_cpu_requests="500m",
            head_cpu_limits="500m",
            head_memory_requests=2,
            head_memory_limits=3,
            num_workers=1,
            worker_cpu_requests="500m",
            worker_cpu_limits="500m",
            worker_memory_requests=1,
            worker_memory_limits=2,
            image=get_ray_image(),
        )

        rayjob = RayJob(
            job_name=job_name,
            namespace=self.namespace,
            cluster_config=cluster_config,
            entrypoint="python -c \"import ray; ray.init(); print('Non-Kueue job done')\"",
            runtime_env={"env_vars": get_setup_env_variables(ACCELERATOR="cpu")},
            # No local_queue - not Kueue managed
        )

        try:
            assert rayjob.submit() == job_name
            assert self.job_api.wait_until_job_running(
                name=rayjob.name, k8s_namespace=rayjob.namespace, timeout=300
            )

            # Non-Kueue jobs can be suspended
            assert rayjob.stop() is True
            assert wait_for_job_status(
                self.job_api, rayjob.name, rayjob.namespace, "Suspended", timeout=30
            )

            # Resubmit
            assert rayjob.resubmit() is True
            assert self.job_api.wait_until_job_finished(
                name=rayjob.name, k8s_namespace=rayjob.namespace, timeout=300
            )
        finally:
            assert rayjob.delete()
            verify_rayjob_cluster_cleanup(cluster_api, rayjob.name, rayjob.namespace)

    def test_lifecycled_kueue_resource_queueing(self):
        """Test Kueue resource queueing with lifecycled clusters."""
        self.setup_method()
        create_namespace(self)
        create_limited_kueue_resources(self)

        self.job_api = RayjobApi()
        cluster_api = RayClusterApi()

        cluster_config = ManagedClusterConfig(
            head_cpu_requests="2",
            head_cpu_limits="3",
            head_memory_requests=6,
            head_memory_limits=8,
            num_workers=0,
            image=get_ray_image(),
        )

        job1 = None
        job2 = None
        try:
            job1 = RayJob(
                job_name="holder",
                namespace=self.namespace,
                cluster_config=cluster_config,
                entrypoint='python -c "import ray; import time; ray.init(); time.sleep(15)"',
                runtime_env={"env_vars": get_setup_env_variables(ACCELERATOR="cpu")},
                local_queue=self.local_queues[0],
            )
            assert job1.submit() == "holder"
            assert self.job_api.wait_until_job_running(
                name=job1.name, k8s_namespace=job1.namespace, timeout=60
            )

            job2 = RayJob(
                job_name="waiter",
                namespace=self.namespace,
                cluster_config=cluster_config,
                entrypoint='python -c "import ray; ray.init()"',
                runtime_env={"env_vars": get_setup_env_variables(ACCELERATOR="cpu")},
                local_queue=self.local_queues[0],
            )
            assert job2.submit() == "waiter"

            sleep(3)
            job2_cr = self.job_api.get_job(name=job2.name, k8s_namespace=job2.namespace)
            assert job2_cr.get("spec", {}).get("suspend", False)

            assert self.job_api.wait_until_job_finished(
                name=job1.name, k8s_namespace=job1.namespace, timeout=60
            )

            assert wait_for_kueue_admission(
                self, self.job_api, job2.name, job2.namespace, timeout=30
            )

            assert self.job_api.wait_until_job_finished(
                name=job2.name, k8s_namespace=job2.namespace, timeout=60
            )
        finally:
            for job in [job1, job2]:
                if job:
                    try:
                        job.delete()
                        verify_rayjob_cluster_cleanup(
                            cluster_api, job.name, job.namespace
                        )
                    except:
                        pass
