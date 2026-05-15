use std::collections::HashMap;

pub(in crate::engineer) fn apply_indicator_warmups(
    bools: &mut HashMap<&'static str, Vec<bool>>,
    floats: &mut HashMap<&'static str, Vec<f64>>,
) {
    // Indicators already encode their warmup rows with NaNs. Keeping this pass
    // empty avoids maintaining two separate warmup policies.
    let _ = (bools, floats);
}
