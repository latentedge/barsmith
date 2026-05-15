mod candle;
mod derived;
mod oscillators;
mod price;
mod trend;
mod volatility;
mod warmup;

pub(super) use candle::candle_features;
pub(super) use derived::DerivedMetrics;
pub(super) use oscillators::{bollinger_features, macd_features, oscillator_features};
pub(super) use price::PriceSeries;
pub(super) use trend::{ema_price_features, kalman_features, trend_state_features};
pub(super) use volatility::volatility_features;
pub(super) use warmup::apply_indicator_warmups;

const SMALL_DIVISOR: f64 = 1e-9;
