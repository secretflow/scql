#!/bin/bash
# OPENSOURCE-CLEANUP DELETE_FILE

set -ex

INPUT_DIR=$1
OUTPUT_DIR=$2

function CombineCoverageReportAndGenHTML() {
  local output_directory="$2"
  # var path is bazel-testlogs, where coverage.dat files were stored
  path="$1/bazel-testlogs/"

  # py unittest generate empty coverage.dat and should be deleted
  # TODO(jint), fix python coverage.
  find ${path} -name "coverage.dat" -size 0 -delete

  datfiles=$(find ${path} -name "coverage.dat")
  for file in ${datfiles}; do
    mv "${file}" "${file}.bk"
    # delete files that we dont't need to calculate the coverage
    lcov --remove "${file}.bk" '*/third_party/*' '*/external/*' '*/usr/*' '*.pb.h' '*.pb.cc' -o "${file}"
    chmod 777 "${file}"
  done

  # delete empty coverage report again
  find ${path} -name "coverage.dat" -size 0 -delete
  datfiles=$(find ${path} -name "coverage.dat")
  lcov_args=""
  for file in ${datfiles}; do
    lcov_args="${lcov_args} -a ${file}"
  done
  lcov ${lcov_args} -o ${output_directory}/combined.dat
  rm -f coverage.xml
  curl http://aivolvo-dev.cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com/citools/lcov_cobertura.py -o /tmp/lcov_cobertura.py
  python /tmp/lcov_cobertura.py ${output_directory}/combined.dat --output ${output_directory}/coverage.xml --demangle
  # add --branch-coverage to show branch coverage
  genhtml --title "Code Coverage for SCQL Engine" --ignore-errors "source" ${datfiles} -o "${output_directory}/html"
}

CombineCoverageReportAndGenHTML ${INPUT_DIR} ${OUTPUT_DIR}
