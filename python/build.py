#!/usr/bin/env python3
# Copyright 2025 Ant Group Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""SCQL Python Package Build Script

This script builds the SCQL Python package components:
- C++ Engine: Built with Bazel in Docker (default, for glibc compatibility)
- Go Compiler: Built with gopy locally

Usage:
    python build.py           # Build all components (engine in Docker, compiler locally)
    python build.py engine    # Build only engine (engine in Docker)
    python build.py compiler  # Build only compiler (locally)
    python build.py clean     # Clean build artifacts

Advanced Options:
    --no-docker               # Build engine locally (not recommended)
    --docker-image IMAGE      # Use custom Docker image
    --docker-name NAME        # Use custom container name
    --docker-restart          # Force restart Docker container

Examples:
    python build.py                    # Default: engine in Docker, compiler locally
    python build.py engine             # Build only engine in Docker
    python build.py compiler           # Build only compiler locally
    python build.py --no-docker        # Build everything locally (glibc compatibility issues)
    python build.py --docker-restart   # Build with fresh Docker container
"""

import os
import subprocess
import sys
import shutil
import tempfile
import argparse
from pathlib import Path


# Configuration
PYTHON_ROOT = Path(__file__).parent.absolute()
SCQL_ROOT = PYTHON_ROOT.parent
SRC_DIR = PYTHON_ROOT / "src"
SRC_PKG_DIR = SRC_DIR / "scql"
BUILD_DIR = PYTHON_ROOT / "build"
PACKAGE_DIR = PYTHON_ROOT / "package"

# Source directories
SRC_ENGINE_DIR = SRC_PKG_DIR / "engine"
SRC_COMPILER_DIR = SRC_PKG_DIR / "compiler"

# Build directories
BUILD_ENGINE_DIR = BUILD_DIR / "engine"
BUILD_COMPILER_DIR = BUILD_DIR / "compiler"

# Package directories
PKG_ENGINE_DIR = PACKAGE_DIR / "scql" / "engine"
PKG_COMPILER_DIR = PACKAGE_DIR / "scql" / "compiler"

# Build configuration
PYTHON_VM = "python3"
GO_PACKAGE_PATH = "./pkg/interpreter/sc"

# Docker configuration
CONTAINER_NAME = f"scql-dev-{os.getenv('USER', 'unknown')}"


class DockerEngineBuilder:
    """Manages Docker container for building SCQL engine to avoid glibc compatibility issues."""

    def __init__(self, scql_root, container_name=None, image=None):
        self.scql_root = Path(scql_root)
        self.container_name = container_name or CONTAINER_NAME
        self.image = image
        self.bazel_cache_dir = Path.home() / ".cache" / "bazel"
        self.bazelisk_cache_dir = Path.home() / ".cache" / "bazelisk"

    def ensure_container_running(self, force_restart=False):
        """Ensure the Docker container is running."""
        # Check if container exists
        result = run_command(
            ["docker", "ps", "-a", "--filter", f"name={self.container_name}"],
            capture_output=True,
        )

        if result.returncode == 0 and self.container_name in result.stdout:
            # Container exists
            if force_restart:
                print(f"Removing existing container: {self.container_name}")
                run_command(["docker", "rm", "-f", self.container_name])
                return self.create_and_start_container()
            else:
                # Check if it's running
                result = run_command(
                    ["docker", "ps", "--filter", f"name={self.container_name}"],
                    capture_output=True,
                )
                if result.returncode == 0 and self.container_name in result.stdout:
                    print(f"Container {self.container_name} is already running")
                    return True
                else:
                    print(f"Starting existing container: {self.container_name}")
                    run_command(["docker", "start", self.container_name])
                    return True
        else:
            # Container doesn't exist, create it
            return self.create_and_start_container()

    def create_and_start_container(self):
        """Create and start a new Docker container."""
        print(f"Creating new container: {self.container_name}")

        # Ensure cache directories exist
        self.bazel_cache_dir.mkdir(parents=True, exist_ok=True)
        self.bazelisk_cache_dir.mkdir(parents=True, exist_ok=True)

        # Docker run command
        cmd = [
            "docker",
            "run",
            "-d",
            "-it",
            "--name",
            self.container_name,
            "--mount",
            f"type=bind,source={self.scql_root},target=/home/admin/dev/",
            "--mount",
            f"type=bind,source={self.bazel_cache_dir},target=/root/.cache/bazel",
            "--mount",
            f"type=bind,source={self.bazelisk_cache_dir},target=/root/.cache/bazelisk",
            "-w",
            "/home/admin/dev",
            "--cap-add=SYS_PTRACE",
            "--security-opt",
            "seccomp=unconfined",
            "--cap-add=NET_ADMIN",
            "--privileged=true",
            self.image,
            "/bin/bash",
        ]

        try:
            run_command(cmd)
            print(
                f"✅ Container {self.container_name} created and started successfully"
            )
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to create container: {e}")
            return False

    def execute_in_container(self, cmd, cwd="/home/admin/dev"):
        """Execute command inside the Docker container."""
        docker_cmd = ["docker", "exec", self.container_name] + cmd
        return run_command(docker_cmd, cwd=None)  # cwd is relative to container

    def copy_from_container(self, container_path, host_path):
        """Copy file from container to host."""
        cmd = [
            "docker",
            "cp",
            f"{self.container_name}:{container_path}",
            str(host_path),
        ]
        run_command(cmd)


class BazelRcManager:
    """Context manager to temporarily modify .bazelrc for engine build."""

    def __init__(self, scql_root, use_docker=False, docker_builder=None):
        self.scql_root = Path(scql_root)
        self.bazelrc_path = self.scql_root / ".bazelrc"
        self.backup_path = None
        self.tls_option = '# Avoid static TLS error for pybind11\nbuild --copt="-ftls-model=global-dynamic"'
        self.use_docker = use_docker
        self.docker_builder = docker_builder

    def __enter__(self):
        """Backup and modify .bazelrc"""
        if self.use_docker:
            # In Docker mode, modify .bazelrc inside the container
            container_bazelrc = "/home/admin/dev/.bazelrc"

            # Backup existing .bazelrc in container
            result = run_command(
                [
                    "docker",
                    "exec",
                    self.docker_builder.container_name,
                    "test",
                    "-f",
                    container_bazelrc,
                ],
                capture_output=True,
                check=False,
            )
            if result.returncode == 0:
                run_command(
                    [
                        "docker",
                        "exec",
                        self.docker_builder.container_name,
                        "cp",
                        container_bazelrc,
                        f"{container_bazelrc}.bak",
                    ]
                )
                print("Backed up .bazelrc in container")

            # Add TLS option to .bazelrc in container
            run_command(
                [
                    "docker",
                    "exec",
                    self.docker_builder.container_name,
                    "sh",
                    "-c",
                    f"echo '{self.tls_option}' >> {container_bazelrc}",
                ]
            )
            print("Added temporary TLS option to .bazelrc in container")
        else:
            # Local mode - original logic
            if self.bazelrc_path.exists():
                self.backup_path = self.bazelrc_path.with_suffix(".bak")
                shutil.copy2(self.bazelrc_path, self.backup_path)
                print(f"Backed up .bazelrc to {self.backup_path}")

            # Add TLS option to .bazelrc
            with open(self.bazelrc_path, "a") as f:
                f.write(f"\n{self.tls_option}\n")
            print("Added temporary TLS option to .bazelrc")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore original .bazelrc"""
        if self.use_docker:
            # Restore .bazelrc in container
            container_bazelrc = "/home/admin/dev/.bazelrc"

            result = run_command(
                [
                    "docker",
                    "exec",
                    self.docker_builder.container_name,
                    "test",
                    "-f",
                    f"{container_bazelrc}.bak",
                ],
                capture_output=True,
                check=False,
            )
            if result.returncode == 0:
                run_command(
                    [
                        "docker",
                        "exec",
                        self.docker_builder.container_name,
                        "cp",
                        f"{container_bazelrc}.bak",
                        container_bazelrc,
                    ]
                )
                run_command(
                    [
                        "docker",
                        "exec",
                        self.docker_builder.container_name,
                        "rm",
                        f"{container_bazelrc}.bak",
                    ]
                )
                print("Restored original .bazelrc in container")
            elif (
                run_command(
                    [
                        "docker",
                        "exec",
                        self.docker_builder.container_name,
                        "test",
                        "-f",
                        container_bazelrc,
                    ],
                    capture_output=True,
                    check=False,
                ).returncode
                == 0
            ):
                # Remove the added TLS option
                result = run_command(
                    [
                        "docker",
                        "exec",
                        self.docker_builder.container_name,
                        "cat",
                        container_bazelrc,
                    ],
                    capture_output=True,
                )
                if result.returncode == 0:
                    content = result.stdout
                    lines = content.split("\n")
                    filtered_lines = []
                    skip_next = False
                    for line in lines:
                        if "# Avoid static TLS error for pybind11" in line:
                            skip_next = True
                            continue
                        elif (
                            skip_next
                            and 'build --copt="-ftls-model=global-dynamic"' in line
                        ):
                            skip_next = False
                            continue
                        filtered_lines.append(line)

                    # Write back to file
                    filtered_content = "\n".join(filtered_lines)
                    run_command(
                        [
                            "docker",
                            "exec",
                            self.docker_builder.container_name,
                            "sh",
                            "-c",
                            f"echo '{filtered_content}' > {container_bazelrc}",
                        ]
                    )
                    print("Removed temporary TLS option from .bazelrc in container")
        else:
            # Local mode - original logic
            if self.backup_path and self.backup_path.exists():
                shutil.copy2(self.backup_path, self.bazelrc_path)
                self.backup_path.unlink()
                print("Restored original .bazelrc")
            elif self.bazelrc_path.exists():
                # Remove the added TLS option
                with open(self.bazelrc_path, "r") as f:
                    content = f.read()

                # Remove the TLS option we added
                lines = content.split("\n")
                filtered_lines = []
                skip_next = False
                for line in lines:
                    if "# Avoid static TLS error for pybind11" in line:
                        skip_next = True
                        continue
                    elif (
                        skip_next
                        and 'build --copt="-ftls-model=global-dynamic"' in line
                    ):
                        skip_next = False
                        continue
                    filtered_lines.append(line)

                with open(self.bazelrc_path, "w") as f:
                    f.write("\n".join(filtered_lines))
                print("Removed temporary TLS option from .bazelrc")


def run_command(cmd, cwd=None, check=True, capture_output=False):
    """Run a shell command with logging."""
    cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
    print(f"Running: {cmd_str}")

    if capture_output:
        result = subprocess.run(
            cmd, cwd=cwd, check=check, capture_output=True, text=True
        )
        return result
    else:
        subprocess.run(cmd, cwd=cwd, check=check)


def ensure_directory(path):
    """Ensure directory exists."""
    path.mkdir(parents=True, exist_ok=True)


def build_cpp_engine(use_docker=False):
    """Build SCQL C++ engine using Bazel."""
    print("\n>>> [1/3] Building C++ Engine with Bazel...")

    if use_docker:
        print(
            "🐳 Using Docker container for engine compilation (avoids glibc compatibility issues)"
        )
    else:
        print("🔧 Using local environment for engine compilation")

    os.chdir(SCQL_ROOT)

    if use_docker:
        # Use global Docker builder if available (when called from main with args.docker)
        docker_builder = globals().get("docker_builder")
        if docker_builder is None:
            # Initialize Docker builder for standalone calls
            docker_builder = DockerEngineBuilder(SCQL_ROOT)

            # Ensure container is running
            if not docker_builder.ensure_container_running():
                raise RuntimeError("Failed to start Docker container")

        # Temporarily modify .bazelrc in container to add TLS option
        with BazelRcManager(SCQL_ROOT, use_docker=True, docker_builder=docker_builder):
            # Build with Bazel in container
            docker_builder.execute_in_container(
                [
                    "bazelisk",
                    "build",
                    "//python/engine:scql_engine",
                    "-c",
                    "opt",
                ]
            )

        # Copy artifacts from container to host
        container_build_path = "/home/admin/dev/bazel-bin/python/engine/scql_engine.so"
        target_file = BUILD_ENGINE_DIR / "scql_engine.so"

        ensure_directory(BUILD_ENGINE_DIR)
        docker_builder.copy_from_container(container_build_path, target_file)

        # Verify file was copied
        if not target_file.exists():
            raise FileNotFoundError(
                f"Engine artifact not found after copy: {target_file}"
            )

        print(f"Built engine to: {target_file}")
        print("✅ Engine compiled in Docker container for better glibc compatibility")
    else:
        # Local compilation (original logic)
        with BazelRcManager(SCQL_ROOT, use_docker=False):
            # Build with Bazel
            run_command(
                [
                    "bazelisk",
                    "build",
                    "//python/engine:scql_engine",
                    "-c",
                    "opt",
                ]
            )

        # Copy artifacts to build directory
        bazel_bin = SCQL_ROOT / "bazel-bin" / "python" / "engine"
        source_file = bazel_bin / "scql_engine.so"
        target_file = BUILD_ENGINE_DIR / "scql_engine.so"

        if not source_file.exists():
            raise FileNotFoundError(f"Engine artifact not found: {source_file}")

        ensure_directory(BUILD_ENGINE_DIR)
        shutil.copy2(source_file, target_file)
        print(f"Built engine to: {target_file}")


def build_go_compiler():
    """Build SCQL Go compiler using gopy."""
    print("\n>>> [2/3] Building Go Compiler with gopy...")

    os.chdir(SCQL_ROOT)

    # Install gopy if not present
    try:
        run_command(["gopy", "--version"], capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Installing gopy...")
        run_command(["go", "install", "github.com/go-python/gopy@latest"])

    # Build Go bindings
    ensure_directory(BUILD_COMPILER_DIR)
    run_command(
        [
            "gopy",
            "build",
            "-output",
            str(BUILD_COMPILER_DIR),
            "-vm",
            PYTHON_VM,
            "-name",
            "scql_compiler",
            GO_PACKAGE_PATH,
        ]
    )

    print(f"Built compiler to: {BUILD_COMPILER_DIR}")


def create_package_directory():
    """Create package directory by copying source files."""
    print("\n>>> [3/4] Creating package directory...")

    if PACKAGE_DIR.exists():
        shutil.rmtree(PACKAGE_DIR)

    # Copy source directory to package
    shutil.copytree(SRC_DIR, PACKAGE_DIR)
    print(f"Copied source to: {PACKAGE_DIR}")


def copy_artifacts():
    """Copy artifacts from build directory to package directory."""
    print("\n>>> [4/4] Copying artifacts to package directory...")

    # Copy engine
    engine_source = BUILD_ENGINE_DIR / "scql_engine.so"
    engine_target = PKG_ENGINE_DIR / "scql_engine.so"

    if engine_source.exists():
        ensure_directory(PKG_ENGINE_DIR)
        shutil.copy2(engine_source, engine_target)
        print(f"Copied engine to: {engine_target}")
    else:
        print(f"Warning: Engine artifact not found: {engine_source}")
        return False

    # Copy compiler - find actual .so file generated by gopy
    compiler_sources = list(BUILD_COMPILER_DIR.glob("*.so"))

    if not compiler_sources:
        print(f"Warning: No compiler .so files found in {BUILD_COMPILER_DIR}")
        return False

    # Use the first .so file found and rename it
    compiler_source = compiler_sources[0]
    compiler_target = PKG_COMPILER_DIR / "scql_compiler.so"

    ensure_directory(PKG_COMPILER_DIR)
    shutil.copy2(compiler_source, compiler_target)
    print(f"Copied compiler to: {compiler_target}")
    print(f"  Source: {compiler_source}")

    # Copy any .py files gopy might generate, except __init__.py (keep our modified version)
    for py_file in BUILD_COMPILER_DIR.glob("*.py"):
        if py_file.name == "__init__.py":
            print(f"Skipping {py_file.name} - keeping our modified version")
            continue
        py_target = PKG_COMPILER_DIR / py_file.name
        shutil.copy2(py_file, py_target)
        print(f"Copied compiler Python wrapper: {py_target}")

    print("✅ All artifacts copied to package directory")
    return True


def clean_artifacts():
    """Clean build artifacts."""
    print("\n>>> Cleaning build artifacts...")

    # Clean build directory
    if BUILD_DIR.exists():
        print(f"Removing build directory: {BUILD_DIR}")
        shutil.rmtree(BUILD_DIR)

    # Clean package directory
    if PACKAGE_DIR.exists():
        print(f"Removing package directory: {PACKAGE_DIR}")
        shutil.rmtree(PACKAGE_DIR)


def main():
    """Main build function."""
    parser = argparse.ArgumentParser(
        description="Build SCQL Python package (engine in Docker, compiler locally)"
    )

    # Positional arguments
    parser.add_argument(
        "component",
        choices=["all", "engine", "compiler", "clean"],
        nargs="?",
        default="all",
        help="Component to build (default: all)",
    )

    # Docker options for advanced users
    parser.add_argument(
        "--docker-image",
        help=f"Docker image to use",
    )
    parser.add_argument(
        "--docker-name", help=f"Docker container name (default: {CONTAINER_NAME})"
    )
    parser.add_argument(
        "--docker-restart",
        action="store_true",
        help="Force restart Docker container if it exists",
    )
    parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Build engine locally (not recommended due to glibc compatibility issues)",
    )

    args = parser.parse_args()

    print("SCQL Python Package Build Script")
    print("=" * 50)

    # By default, use Docker for engine compilation unless explicitly disabled
    use_docker = not args.no_docker and args.component in ["all", "engine"]

    if use_docker:
        print(
            "🐳 Engine will be built in Docker container for better glibc compatibility"
        )

    # Global Docker configuration for containers
    docker_builder = None
    if use_docker:
        container_name = args.docker_name or CONTAINER_NAME
        docker_builder = DockerEngineBuilder(
            SCQL_ROOT, container_name, args.docker_image
        )

        # Ensure container is running with restart option if needed
        if not docker_builder.ensure_container_running(
            force_restart=args.docker_restart
        ):
            print("❌ Failed to start Docker container")
            sys.exit(1)

    try:
        if args.component == "clean":
            clean_artifacts()
            return

        if args.component in ["all", "engine"]:
            build_cpp_engine(use_docker=use_docker)

        if args.component in ["all", "compiler"]:
            build_go_compiler()

        if args.component == "all":
            create_package_directory()
            copy_result = copy_artifacts()
        elif args.component in ["engine", "compiler"]:
            print("\n📦 Build artifacts ready in build directory:")
            if BUILD_ENGINE_DIR.exists():
                print(f"  Engine: {BUILD_ENGINE_DIR}/")
            if BUILD_COMPILER_DIR.exists():
                print(f"  Compiler: {BUILD_COMPILER_DIR}/")
            copy_result = True
        else:
            copy_result = False

        if copy_result and args.component == "all":
            # Build wheel package
            print("\n>>> Building wheel package...")
            os.chdir(PACKAGE_DIR)

            # Build wheel (python -m build will create .whl in a dist/ subdirectory)
            run_command(["python", "-m", "build", "--wheel", "."])

            # Fix wheel to include .so files
            wheel_dir = PACKAGE_DIR / "dist"
            wheel_files = list(wheel_dir.glob("*.whl")) if wheel_dir.exists() else []
            if wheel_files:
                original_wheel = wheel_files[0]

                # Extract version from the original wheel filename
                original_name = original_wheel.stem  # Get name without .whl extension
                # Name format is: scql-{version}-{python_tag}-{abi_tag}-{platform_tag}
                # Example: scql-0.1.0.dev20251229-py311-none-any
                parts = original_name.split("-")
                version_part = parts[1]  # The second part should be the version
                fixed_wheel = PACKAGE_DIR / f"scql-{version_part}-py311-none-any.whl"

                with tempfile.TemporaryDirectory() as temp_dir:
                    temp_dir = Path(temp_dir)

                    # Extract wheel
                    run_command(
                        ["unzip", "-q", str(original_wheel), "-d", str(temp_dir)]
                    )

                    # Copy .so files to wheel
                    engine_so = PKG_ENGINE_DIR / "scql_engine.so"
                    compiler_so = PKG_COMPILER_DIR / "scql_compiler.so"

                    if engine_so.exists():
                        shutil.copy2(
                            engine_so, temp_dir / "scql" / "engine" / "scql_engine.so"
                        )
                        print("✅ Added scql_engine.so to wheel")

                    if compiler_so.exists():
                        shutil.copy2(
                            compiler_so,
                            temp_dir / "scql" / "compiler" / "scql_compiler.so",
                        )
                        print("✅ Added scql_compiler.so to wheel")

                    # Create fixed wheel
                    old_cwd = os.getcwd()
                    os.chdir(temp_dir)
                    run_command(["zip", "-r", str(fixed_wheel.name), "."])
                    # Move the created wheel to the correct location
                    temp_wheel = temp_dir / fixed_wheel.name
                    if temp_wheel.exists():
                        shutil.move(str(temp_wheel), str(fixed_wheel))
                    os.chdir(old_cwd)

                    # Remove original wheel
                original_wheel.unlink()

                print("\n✅ Build completed successfully!")
                print("\n📦 Distribution package created:")
                print(f"  Build artifacts: {BUILD_DIR}/")
                print(f"  Package directory: {PACKAGE_DIR}/")
                print(f"  Installable wheel: {fixed_wheel}")
                print("\n🚀 Installation:")
                print(f"  cd {PACKAGE_DIR}")
                print(f"  uv pip install {fixed_wheel.name}")
                print("\n📋 The wheel includes all required components:")
                print(
                    "  • SCQL compiler with ideal interface (import scql.compiler as sc)"
                )
                print("  • SCQL engine for secure execution")
                print("  • All native binary libraries (.so files)")
            else:
                print("❌ No wheel file created!")
                sys.exit(1)
        elif copy_result:
            print("\n✅ Partial build completed!")
        else:
            print("\n❌ Build failed - no artifacts found!")
            sys.exit(1)

    except subprocess.CalledProcessError as e:
        print("\n❌ Build failed with error:")
        print(f"Command: {' '.join(e.cmd) if e.cmd else 'Unknown'}")
        print(f"Return code: {e.returncode}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Build failed with exception: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
