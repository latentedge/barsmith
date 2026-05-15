pub fn build_long_levels(
    close: &[f64],
    low: &[f64],
    atr_values: &[f64],
    tp_multiple: f64,
) -> (Vec<f64>, Vec<f64>) {
    let len = close.len().min(low.len()).min(atr_values.len());
    let mut stop = vec![f64::NAN; close.len()];
    let mut tp = vec![f64::NAN; close.len()];
    for i in 0..len {
        let entry = close[i];
        let atr = atr_values[i];
        let bar_low = low[i];
        if !entry.is_finite() || !atr.is_finite() || !bar_low.is_finite() {
            continue;
        }
        let atr_stop = entry - atr;
        let mut sl = f64::NAN;
        if bar_low < entry {
            sl = bar_low;
        }
        if atr_stop < entry && (!sl.is_finite() || atr_stop > sl) {
            sl = atr_stop;
        }
        if sl.is_finite() {
            stop[i] = sl;
            tp[i] = entry + tp_multiple * atr;
        }
    }
    (stop, tp)
}

pub fn build_short_levels(
    close: &[f64],
    high: &[f64],
    atr_values: &[f64],
    tp_multiple: f64,
) -> (Vec<f64>, Vec<f64>) {
    let len = close.len().min(high.len()).min(atr_values.len());
    let mut stop = vec![f64::NAN; close.len()];
    let mut tp = vec![f64::NAN; close.len()];
    for i in 0..len {
        let entry = close[i];
        let atr = atr_values[i];
        let bar_high = high[i];
        if !entry.is_finite() || !atr.is_finite() || !bar_high.is_finite() {
            continue;
        }
        let atr_stop = entry + atr;
        let mut sl = f64::NAN;
        if bar_high > entry {
            sl = bar_high;
        }
        if atr_stop > entry && (!sl.is_finite() || atr_stop < sl) {
            sl = atr_stop;
        }
        if sl.is_finite() {
            stop[i] = sl;
            tp[i] = entry - tp_multiple * atr;
        }
    }
    (stop, tp)
}
