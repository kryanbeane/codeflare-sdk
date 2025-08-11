# Copyright 2025 IBM, Red Hat
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
RayCluster spec builder specifically for RayJobs.

This module builds the rayClusterSpec portion of a RayJob CR, which defines
how the Ray cluster should be created when the job runs.
"""

import copy
import logging
from typing import Dict, Any, Union

from kubernetes.client import (
    V1ObjectMeta,
    V1Container,
    V1ContainerPort,
    V1Lifecycle,
    V1ExecAction,
    V1LifecycleHandler,
    V1EnvVar,
    V1PodTemplateSpec,
    V1PodSpec,
    V1ResourceRequirements,
    V1Volume,
    V1VolumeMount,
    V1ConfigMapVolumeSource,
    V1KeyToPath,
)

from codeflare_sdk.ray.rayjobs.config import RayJobClusterConfig

from ...common.utils.constants import CUDA_RUNTIME_IMAGE

logger = logging.getLogger(__name__)

# Default volume mounts for CA certificates
DEFAULT_VOLUME_MOUNTS = [
    V1VolumeMount(
        mount_path="/etc/pki/tls/certs/odh-trusted-ca-bundle.crt",
        name="odh-trusted-ca-cert",
        sub_path="odh-trusted-ca-bundle.crt",
    ),
    V1VolumeMount(
        mount_path="/etc/ssl/certs/odh-trusted-ca-bundle.crt",
        name="odh-trusted-ca-cert",
        sub_path="odh-trusted-ca-bundle.crt",
    ),
    V1VolumeMount(
        mount_path="/etc/pki/tls/certs/odh-ca-bundle.crt",
        name="odh-ca-cert",
        sub_path="odh-ca-bundle.crt",
    ),
    V1VolumeMount(
        mount_path="/etc/ssl/certs/odh-ca-bundle.crt",
        name="odh-ca-cert",
        sub_path="odh-ca-bundle.crt",
    ),
]

# Default volumes for CA certificates
DEFAULT_VOLUMES = [
    V1Volume(
        name="odh-trusted-ca-cert",
        config_map=V1ConfigMapVolumeSource(
            name="odh-trusted-ca-bundle",
            items=[V1KeyToPath(key="ca-bundle.crt", path="odh-trusted-ca-bundle.crt")],
            optional=True,
        ),
    ),
    V1Volume(
        name="odh-ca-cert",
        config_map=V1ConfigMapVolumeSource(
            name="odh-trusted-ca-bundle",
            items=[V1KeyToPath(key="odh-ca-bundle.crt", path="odh-ca-bundle.crt")],
            optional=True,
        ),
    ),
]


def build_ray_cluster_spec(
    cluster_config: RayJobClusterConfig,
) -> Dict[str, Any]:
    """
    Build the RayCluster spec from RayJobClusterConfig for embedding in RayJob.

    Args:
        cluster_config: The cluster configuration object (RayJobClusterConfig)

    Returns:
        Dict containing the RayCluster spec for embedding in RayJob
    """
    # Create a copy to avoid modifying the original
    temp_config = copy.copy(cluster_config)

    temp_config.appwrapper = False
    temp_config.write_to_file = False

    ray_cluster_spec = {
        "rayVersion": CUDA_RUNTIME_IMAGE,
        "enableInTreeAutoscaling": False,
        "headGroupSpec": _build_head_group_spec(temp_config),
        "workerGroupSpecs": [_build_worker_group_spec(temp_config)],
    }

    # Add GCS fault tolerance if enabled
    if temp_config.enable_gcs_ft:
        gcs_ft_options = _build_gcs_ft_options(temp_config)
        ray_cluster_spec["gcsFaultToleranceOptions"] = gcs_ft_options

    logger.info(f"Built RayCluster spec for cluster: {cluster_config.name}")
    return ray_cluster_spec


def _build_head_group_spec(cluster_config: RayJobClusterConfig) -> Dict[str, Any]:
    """Build the head group specification."""
    return {
        "serviceType": "ClusterIP",
        "enableIngress": False,
        "rayStartParams": _build_head_ray_params(cluster_config),
        "template": V1PodTemplateSpec(
            metadata=V1ObjectMeta(
                annotations=(
                    cluster_config.annotations
                    if hasattr(cluster_config, "annotations")
                    else None
                )
            ),
            spec=_build_pod_spec(
                cluster_config, _build_head_container(cluster_config), is_head=True
            ),
        ),
    }


def _build_worker_group_spec(cluster_config: RayJobClusterConfig) -> Dict[str, Any]:
    """Build the worker group specification."""
    return {
        "replicas": cluster_config.num_workers,
        "minReplicas": cluster_config.num_workers,
        "maxReplicas": cluster_config.num_workers,
        "groupName": f"worker-group-{cluster_config.name}",
        "rayStartParams": _build_worker_ray_params(cluster_config),
        "template": V1PodTemplateSpec(
            metadata=V1ObjectMeta(
                annotations=(
                    cluster_config.annotations
                    if hasattr(cluster_config, "annotations")
                    else None
                )
            ),
            spec=_build_pod_spec(
                cluster_config, _build_worker_container(cluster_config), is_head=False
            ),
        ),
    }


def _build_head_ray_params(cluster_config: RayJobClusterConfig) -> Dict[str, str]:
    """Build Ray start parameters for head node."""
    params = {
        "dashboard-host": "0.0.0.0",
        "dashboard-port": "8265",
        "block": "true",
    }

    # Add GPU count if specified
    if (
        hasattr(cluster_config, "head_accelerators")
        and cluster_config.head_accelerators
    ):
        gpu_count = sum(
            count
            for resource_type, count in cluster_config.head_accelerators.items()
            if "gpu" in resource_type.lower()
        )
        if gpu_count > 0:
            params["num-gpus"] = str(gpu_count)

    return params


def _build_worker_ray_params(cluster_config: RayJobClusterConfig) -> Dict[str, str]:
    """Build Ray start parameters for worker nodes."""
    params = {
        "block": "true",
    }

    # Add GPU count if specified
    if (
        hasattr(cluster_config, "worker_accelerators")
        and cluster_config.worker_accelerators
    ):
        gpu_count = sum(
            count
            for resource_type, count in cluster_config.worker_accelerators.items()
            if "gpu" in resource_type.lower()
        )
        if gpu_count > 0:
            params["num-gpus"] = str(gpu_count)

    return params


def _build_head_container(cluster_config: RayJobClusterConfig) -> V1Container:
    """Build the head container specification."""
    container = V1Container(
        name="ray-head",
        image=cluster_config.image or CUDA_RUNTIME_IMAGE,
        image_pull_policy="IfNotPresent",  # Always IfNotPresent for RayJobs
        ports=[
            V1ContainerPort(name="gcs", container_port=6379),
            V1ContainerPort(name="dashboard", container_port=8265),
            V1ContainerPort(name="client", container_port=10001),
        ],
        lifecycle=V1Lifecycle(
            pre_stop=V1LifecycleHandler(
                _exec=V1ExecAction(command=["/bin/sh", "-c", "ray stop"])
            )
        ),
        resources=_build_resource_requirements(
            cluster_config.head_cpu_requests,
            cluster_config.head_cpu_limits,
            cluster_config.head_memory_requests,
            cluster_config.head_memory_limits,
            cluster_config.head_accelerators,
        ),
        volume_mounts=_generate_volume_mounts(cluster_config),
    )

    # Add environment variables if specified
    if hasattr(cluster_config, "envs") and cluster_config.envs:
        container.env = _build_env_vars(cluster_config.envs)

    return container


def _build_worker_container(cluster_config: RayJobClusterConfig) -> V1Container:
    """Build the worker container specification."""
    container = V1Container(
        name="ray-worker",
        image=cluster_config.image or CUDA_RUNTIME_IMAGE,
        image_pull_policy="IfNotPresent",  # Always IfNotPresent for RayJobs
        lifecycle=V1Lifecycle(
            pre_stop=V1LifecycleHandler(
                _exec=V1ExecAction(command=["/bin/sh", "-c", "ray stop"])
            )
        ),
        resources=_build_resource_requirements(
            cluster_config.worker_cpu_requests,
            cluster_config.worker_cpu_limits,
            cluster_config.worker_memory_requests,
            cluster_config.worker_memory_limits,
            cluster_config.worker_accelerators,
        ),
        volume_mounts=_generate_volume_mounts(cluster_config),
    )

    # Add environment variables if specified
    if hasattr(cluster_config, "envs") and cluster_config.envs:
        container.env = _build_env_vars(cluster_config.envs)

    return container


def _build_resource_requirements(
    cpu_requests: Union[int, str],
    cpu_limits: Union[int, str],
    memory_requests: Union[int, str],
    memory_limits: Union[int, str],
    extended_resource_requests: Dict[str, Union[int, str]] = None,
) -> V1ResourceRequirements:
    """Build Kubernetes resource requirements."""
    resource_requirements = V1ResourceRequirements(
        requests={"cpu": cpu_requests, "memory": memory_requests},
        limits={"cpu": cpu_limits, "memory": memory_limits},
    )

    # Add extended resources (e.g., GPUs)
    if extended_resource_requests:
        for resource_type, amount in extended_resource_requests.items():
            resource_requirements.limits[resource_type] = amount
            resource_requirements.requests[resource_type] = amount

    return resource_requirements


def _build_pod_spec(
    cluster_config: RayJobClusterConfig, container: V1Container, is_head: bool
) -> V1PodSpec:
    """Build the pod specification."""
    pod_spec = V1PodSpec(
        containers=[container],
        volumes=_generate_volumes(cluster_config),
        restart_policy="Never",  # RayJobs should not restart
    )

    # Add tolerations if specified
    if (
        is_head
        and hasattr(cluster_config, "head_tolerations")
        and cluster_config.head_tolerations
    ):
        pod_spec.tolerations = cluster_config.head_tolerations
    elif (
        not is_head
        and hasattr(cluster_config, "worker_tolerations")
        and cluster_config.worker_tolerations
    ):
        pod_spec.tolerations = cluster_config.worker_tolerations

    # Add image pull secrets if specified
    if (
        hasattr(cluster_config, "image_pull_secrets")
        and cluster_config.image_pull_secrets
    ):
        from kubernetes.client import V1LocalObjectReference

        pod_spec.image_pull_secrets = [
            V1LocalObjectReference(name=secret)
            for secret in cluster_config.image_pull_secrets
        ]

    return pod_spec


def _generate_volume_mounts(cluster_config: RayJobClusterConfig) -> list:
    """Generate volume mounts for the container."""
    volume_mounts = DEFAULT_VOLUME_MOUNTS.copy()

    # Add custom volume mounts if specified
    if hasattr(cluster_config, "volume_mounts") and cluster_config.volume_mounts:
        volume_mounts.extend(cluster_config.volume_mounts)

    return volume_mounts


def _generate_volumes(cluster_config: RayJobClusterConfig) -> list:
    """Generate volumes for the pod."""
    volumes = DEFAULT_VOLUMES.copy()

    # Add custom volumes if specified
    if hasattr(cluster_config, "volumes") and cluster_config.volumes:
        volumes.extend(cluster_config.volumes)

    return volumes


def _build_env_vars(envs: Dict[str, str]) -> list:
    """Build environment variables list."""
    return [V1EnvVar(name=key, value=value) for key, value in envs.items()]


def _build_gcs_ft_options(cluster_config: RayJobClusterConfig) -> Dict[str, Any]:
    """Build GCS fault tolerance options."""
    gcs_ft_options = {"redisAddress": cluster_config.redis_address}

    if (
        hasattr(cluster_config, "external_storage_namespace")
        and cluster_config.external_storage_namespace
    ):
        gcs_ft_options[
            "externalStorageNamespace"
        ] = cluster_config.external_storage_namespace

    if (
        hasattr(cluster_config, "redis_password_secret")
        and cluster_config.redis_password_secret
    ):
        gcs_ft_options["redisPassword"] = {
            "valueFrom": {
                "secretKeyRef": {
                    "name": cluster_config.redis_password_secret["name"],
                    "key": cluster_config.redis_password_secret["key"],
                }
            }
        }

    return gcs_ft_options
