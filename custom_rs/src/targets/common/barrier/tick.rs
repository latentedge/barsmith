#[allow(dead_code)]
pub(crate) enum TickRoundMode {
    Nearest,
    Floor,
    Ceil,
}

pub(crate) fn quantize_distance_to_tick(distance: f64, tick_size: f64, mode: TickRoundMode) -> f64 {
    if !distance.is_finite() || tick_size <= 0.0 {
        return distance;
    }
    if distance.abs() < f64::EPSILON {
        return 0.0;
    }

    let ticks = distance / tick_size;
    let raw_rounded = match mode {
        TickRoundMode::Nearest => ticks.round(),
        TickRoundMode::Floor => ticks.floor(),
        TickRoundMode::Ceil => ticks.ceil(),
    };
    // Enforce a minimum of one tick for non-zero distances so that we never
    // end up with a zero-risk trade when a stop is requested.
    let ticks_final = raw_rounded.max(1.0);
    ticks_final * tick_size
}

pub(crate) fn quantize_price_to_tick(price: f64, tick_size: f64, mode: TickRoundMode) -> f64 {
    if !price.is_finite() || tick_size <= 0.0 {
        return price;
    }
    let ticks = price / tick_size;
    let rounded = match mode {
        TickRoundMode::Nearest => ticks.round(),
        TickRoundMode::Floor => ticks.floor(),
        TickRoundMode::Ceil => ticks.ceil(),
    };
    rounded * tick_size
}
