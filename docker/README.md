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
# build image for debug purpose
# It would install golang debug program dlv in image
bash build.sh -s image-dev

# build with anolis based image
bash build.sh -b anolis

# build with ubuntu based image
bash build.sh -b ubuntu
```