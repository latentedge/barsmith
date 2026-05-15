pub fn kalman_filter(values: &[f64], process_var: f64, obs_var: f64) -> (Vec<f64>, Vec<f64>) {
    let len = values.len();
    let mut filtered = vec![f64::NAN; len];
    let mut innovations = vec![0.0; len];
    if len == 0 {
        return (filtered, innovations);
    }

    let mut x = values
        .iter()
        .copied()
        .find(|v| v.is_finite())
        .unwrap_or(0.0);
    let mut p = 1.0;

    for (i, &value) in values.iter().enumerate() {
        let x_pred = x;
        let p_pred = p + process_var;
        if value.is_finite() {
            let k = p_pred / (p_pred + obs_var);
            let innovation = value - x_pred;
            x = x_pred + k * innovation;
            p = (1.0 - k) * p_pred;
            filtered[i] = x;
            innovations[i] = innovation;
        } else {
            filtered[i] = x_pred;
            innovations[i] = 0.0;
            x = x_pred;
            p = p_pred;
        }
    }

    (filtered, innovations)
}
