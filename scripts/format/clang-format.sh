#!/bin/bash

find . -type f \( -name "*.cpp" -o -name "*.cc" -o -name "*.h" -o -name "*.hpp" -o -name "*.proto" \) \
  -exec clang-format-19 -i {} +

echo "All source and proto files formatted with clang-format-19."
