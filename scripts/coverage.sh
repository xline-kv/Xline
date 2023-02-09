#/bin/bash
set -u -e

rm -rf coverage
mkdir coverage

if [[ ! -v CI ]]; then
  FORMAT=html
  OUTPUT_XLINE=coverage/xline_html
  OUTPUT_CURP=coverage/curp_html
  OUTPUT_UTILS=coverage/utils_html
else
  FORMAT=lcov
  OUTPUT_XLINE=coverage/xline_cov.lcovrc
  OUTPUT_CURP=coverage/curp_cov.lcovrc
  OUTPUT_UTILS=coverage/utils_cov.lcovrc
fi

# generate coverage data
CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage' LLVM_PROFILE_FILE='coverage-%p-%m.profraw' cargo test --lib

# generate report
echo "generating xline coverage..."
grcov . \
  --binary-path ./target/debug/ \
  --source-dir ./xline/src \
  -t $FORMAT \
  --branch \
  --ignore-not-existing \
  --excl-br-start '^(pub(\((crate|super)\))? )?mod tests' \
  --excl-br-stop '^}' \
  --ignore="*/tests/*" \
  -o $OUTPUT_XLINE

echo "generating curp coverage..."
grcov . \
  --binary-path ./target/debug/ \
  --source-dir ./curp/src \
  -t $FORMAT \
  --branch \
  --ignore-not-existing \
  --excl-br-start '^(pub(\((crate|super)\))? )?mod tests' \
  --excl-br-stop '^}' \
  --ignore="*/tests/*" \
  -o $OUTPUT_CURP

echo "generating utils coverage..."
grcov . \
  --binary-path ./target/debug/ \
  --source-dir ./utils/src \
  -t $FORMAT \
  --branch \
  --ignore-not-existing \
  --excl-br-start '^(pub(\((crate|super)\))? )?mod tests' \
  --excl-br-stop '^}' \
  --ignore="*/tests/*" \
  -o $OUTPUT_UTILS

# cleanup
echo "cleaning up..."
find . -type f -name '*.profraw' -delete
