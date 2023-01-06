rm -rf coverage
mkdir coverage

if [[ -z "${CI}" ]]; then
  FORMAT=html
  OUTPUT_XLINE=coverage/xline_html
  OUTPUT_CURP=coverage/curp_html
  OUTPUT_LOCK_UTILS=coverage/lock_utils_html
else
  FORMAT=cobertura
  OUTPUT_XLINE=coverage/xline_cov.xml
  OUTPUT_CURP=coverage/curp_cov.xml
  OUTPUT_LOCK_UTILS=coverage/lock_utils_cov.xml
fi

# generate coverage data
CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage' LLVM_PROFILE_FILE='coverage-%p-%m.profraw' cargo test

# generate report
echo "generating xline coverage"
grcov . \
--binary-path ./target/debug/ \
--source-dir ./xline/src \
-t $FORMAT \
--branch \
--ignore-not-existing \
-o $OUTPUT_XLINE

echo "generating curp coverage"
grcov . \
--binary-path ./target/debug/ \
--source-dir ./curp/src \
-t $FORMAT \
--branch \
--ignore-not-existing \
-o $OUTPUT_CURP

echo "generating lock_utils coverage"
grcov . \
--binary-path ./target/debug/ \
--source-dir ./lock_utils/src \
-t $FORMAT \
--branch \
--ignore-not-existing \
-o $OUTPUT_LOCK_UTILS

# cleanup
echo "cleaning up..."
find . -type f -name '*.profraw' -delete
