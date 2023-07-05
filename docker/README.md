# Build

```bash
# build image tagged scql:latest
bash build.sh
# build image tagged scql:${image_tag}
bash build.sh -t image_tag
# build image tagged ${image_name}:${image_tag}
bash build.sh -n image_name -t image_tag

# Advanced
# build image with host bazel cache
bash build.sh -c
```