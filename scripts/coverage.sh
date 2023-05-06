#/bin/bash
set -u -e

rm -rf coverage
mkdir coverage

if [[ ! -v CI ]]; then
  FORMAT=html
  OUTPUT_XLINE=coverage/xline_html
  OUTPUT_CURP=coverage/curp_html
  OUTPUT_UTILS=coverage/utils_html
  OUTPUT_ENGINE=coverage/engine_html
else
  FORMAT=lcov
  OUTPUT_XLINE=coverage/xline_cov.lcovrc
  OUTPUT_CURP=coverage/curp_cov.lcovrc
  OUTPUT_UTILS=coverage/utils_cov.lcovrc
  OUTPUT_ENGINE=coverage/engine_cov.lcovrc
fi

# generate coverage data
CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage' LLVM_PROFILE_FILE='coverage-%p-%m.profraw' cargo test --lib

generate_coverage_report() {
  component=${1}
  format=${2}
  output=${3}
  echo "generating ${component} coverage..."
  grcov . \
    --binary-path ./target/debug/ \
    --source-dir ./${component}/src \
    -t ${format} \
    --branch \
    --ignore-not-existing \
    --excl-br-start '^(pub(\((crate|super)\))? )?mod tests' \
    --excl-br-stop '^}' \
    --ignore="*/tests/*" \
    -o ${output}
}

generate_coverage_report xline $FORMAT $OUTPUT_XLINE
generate_coverage_report curp $FORMAT $OUTPUT_CURP
generate_coverage_report utils $FORMAT $OUTPUT_UTILS
generate_coverage_report engine $FORMAT $OUTPUT_ENGINE

# cleanup
echo "cleaning up..."
find . -type f -name '*.profraw' -delete
