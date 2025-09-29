import requests
from time import sleep

from codeflare_sdk import Cluster, ClusterConfiguration, TokenAuthentication
from codeflare_sdk.ray.client import RayJobClient

import pytest

from support import *

# This test creates a Ray Cluster and covers the Ray Job submission functionality on both Kind and OpenShift


@pytest.mark.kind
@pytest.mark.openshift
class TestRayClusterSDK:
    def setup_method(self):
        initialize_kubernetes_client(self)

    def teardown_method(self):
        delete_namespace(self)
        delete_kueue_resources(self)

    def test_mnist_ray_cluster_sdk(self):
        self.setup_method()
        create_namespace(self)
        create_kueue_resources(self)
        self.run_mnist_raycluster_sdk(accelerator="cpu")

    @pytest.mark.nvidia_gpu
    @pytest.mark.skipif(
        is_openshift(), reason="GPU tests not supported on OpenShift test environment"
    )
    def test_mnist_ray_cluster_sdk_nvidia_gpu(self):
        self.setup_method()
        create_namespace(self)
        create_kueue_resources(self)
        self.run_mnist_raycluster_sdk(accelerator="gpu", number_of_gpus=1)

    def run_mnist_raycluster_sdk(
        self, accelerator, gpu_resource_name="nvidia.com/gpu", number_of_gpus=0
    ):
        # Platform-specific authentication
        if is_openshift():
            auth = TokenAuthentication(
                token=run_oc_command(["whoami", "--show-token=true"]),
                server=run_oc_command(["whoami", "--show-server=true"]),
                skip_tls=True,
            )
            auth.login()

        cluster = Cluster(
            ClusterConfiguration(
                name="mnist",
                namespace=self.namespace,
                num_workers=1,
                head_cpu_requests="500m",
                head_cpu_limits="500m",
                worker_cpu_requests="500m" if not is_openshift() else 1,
                worker_cpu_limits=1,
                worker_memory_requests=1,
                worker_memory_limits=4,
                worker_extended_resource_requests={gpu_resource_name: number_of_gpus},
                image=get_ray_image(),  # Platform-appropriate image
                write_to_file=True,
                verify_tls=False,
            )
        )

        cluster.up()
        cluster.status()
        cluster.wait_ready()
        cluster.status()
        cluster.details()

        # Run platform-specific assertions
        if is_openshift():
            self.assert_jobsubmit_without_auth_fails(cluster)
            self.assert_jobsubmit_with_auth(cluster, accelerator, number_of_gpus)
        else:
            self.assert_jobsubmit_without_auth(cluster, accelerator, number_of_gpus)

        assert_get_cluster_and_jobsubmit(
            self, "mnist", accelerator=accelerator, number_of_gpus=number_of_gpus
        )

    def assert_jobsubmit_without_auth_fails(self, cluster):
        """OpenShift-specific: Test that job submission without auth fails with 403"""
        dashboard_url = cluster.cluster_dashboard_uri()
        jobdata = {
            "entrypoint": "python mnist.py",
            "runtime_env": {
                "working_dir": "./tests/e2e/",
                "pip": "./tests/e2e/mnist_pip_requirements.txt",
                "env_vars": get_setup_env_variables(),
            },
        }
        try:
            response = requests.post(
                dashboard_url + "/api/jobs/", verify=False, json=jobdata
            )
            assert (
                response.status_code == 403
            ), f"Expected 403, got {response.status_code}"
        except Exception as e:
            print(f"An unexpected error occurred. Error: {e}")
            assert False

    def assert_jobsubmit_with_auth(self, cluster, accelerator, number_of_gpus):
        """OpenShift-specific: Test job submission with authentication"""
        auth_token = run_oc_command(["whoami", "--show-token=true"])
        ray_dashboard = cluster.cluster_dashboard_uri()
        header = {"Authorization": f"Bearer {auth_token}"}
        client = RayJobClient(address=ray_dashboard, headers=header, verify=False)

        self._submit_and_wait_for_job(client, accelerator, number_of_gpus)

    def assert_jobsubmit_without_auth(self, cluster, accelerator, number_of_gpus):
        """Kind-specific: Test job submission without authentication"""
        ray_dashboard = cluster.cluster_dashboard_uri()
        client = RayJobClient(address=ray_dashboard, verify=False)

        self._submit_and_wait_for_job(client, accelerator, number_of_gpus)

    def _submit_and_wait_for_job(self, client, accelerator, number_of_gpus):
        """Common job submission and monitoring logic"""
        submission_id = client.submit_job(
            entrypoint="python mnist.py",
            runtime_env={
                "working_dir": "./tests/e2e/",
                "pip": "./tests/e2e/mnist_pip_requirements.txt",
                "env_vars": get_setup_env_variables(ACCELERATOR=accelerator),
            },
            entrypoint_num_gpus=number_of_gpus if not is_openshift() else 0,
            entrypoint_num_cpus=1 if is_openshift() else 0,
        )
        print(f"Submitted job with ID: {submission_id}")

        done = False
        time = 0
        timeout = 900
        while not done:
            status = client.get_job_status(submission_id)
            if status.is_terminal():
                break
            if not done:
                print(status)
                if timeout and time >= timeout:
                    raise TimeoutError(f"job has timed out after waiting {timeout}s")
                sleep(5)
                time += 5

        logs = client.get_job_logs(submission_id)
        print(logs)

        assert status == "SUCCEEDED", f"Job completed with status: {status}"

        client.delete_job(submission_id)
