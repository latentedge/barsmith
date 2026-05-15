use barsmith_indicators::{
    add_scalar, adx, atr, atr_close_to_close, bollinger, derivative, deviation, diff,
    elementwise_max, ema, extension, kalman_filter, lower_wick, momentum, momentum_score, range,
    ratio, ratio_with_eps, roc, rolling_coeff_var, rsi, sma, stochastic, upper_wick, vector_abs,
};

use super::{PriceSeries, SMALL_DIVISOR};

pub(in crate::engineer) struct DerivedMetrics {
    pub(super) abs_body: Vec<f64>,
    pub(super) upper_wick: Vec<f64>,
    pub(super) lower_wick: Vec<f64>,
    pub(super) max_wick: Vec<f64>,
    pub(super) body_to_total_wick: Vec<f64>,
    pub(super) body_atr_ratio: Vec<f64>,
    pub(super) ema9: Vec<f64>,
    pub(super) ema20: Vec<f64>,
    pub(super) ema50: Vec<f64>,
    pub(in crate::engineer) sma200: Vec<f64>,
    pub(super) kf_ma: Vec<f64>,
    pub(super) momentum_14: Vec<f64>,
    pub(super) momentum_score: Vec<f64>,
    pub(super) roc5: Vec<f64>,
    pub(super) roc10: Vec<f64>,
    pub(super) adx: Vec<f64>,
    pub(in crate::engineer) atr: Vec<f64>,
    pub(super) atr_c2c: Vec<f64>,
    pub(super) atr_pct: Vec<f64>,
    pub(super) atr_c2c_pct: Vec<f64>,
    pub(super) bar_range_pct: Vec<f64>,
    pub(super) volatility_20_cv: Vec<f64>,
    pub(super) body_size_pct: Vec<f64>,
    pub(super) upper_shadow_ratio: Vec<f64>,
    pub(super) lower_shadow_ratio: Vec<f64>,
    pub(super) wicks_diff: Vec<f64>,
    pub(super) wicks_diff_sma14: Vec<f64>,
    pub(super) kf_wicks_smooth: Vec<f64>,
    pub(super) price_vs_200sma_dev: Vec<f64>,
    pub(super) price_vs_9ema_dev: Vec<f64>,
    pub(super) nine_to_two_hundred: Vec<f64>,
    pub(super) rsi14: Vec<f64>,
    pub(super) rsi7: Vec<f64>,
    pub(super) rsi21: Vec<f64>,
    pub(super) atr_mean50: Vec<f64>,
    pub(super) atr_c2c_mean50: Vec<f64>,
    pub(super) macd: Vec<f64>,
    pub(super) macd_signal: Vec<f64>,
    pub(super) macd_hist: Vec<f64>,
    pub(super) stoch_k: Vec<f64>,
    pub(super) stoch_d: Vec<f64>,
    pub(super) bb_mid: Vec<f64>,
    pub(super) bb_upper: Vec<f64>,
    pub(super) bb_lower: Vec<f64>,
    pub(super) bb_std: Vec<f64>,
    pub(super) ext: Vec<f64>,
    pub(super) ext_sma14: Vec<f64>,
    pub(super) kf_smooth: Vec<f64>,
    pub(super) kf_innovation: Vec<f64>,
    pub(super) kf_close_momentum: Vec<f64>,
    pub(super) kf_slope_5: Vec<f64>,
    pub(super) kf_trend: Vec<f64>,
    pub(super) kf_adx: Vec<f64>,
    pub(super) kf_atr: Vec<f64>,
    pub(super) kf_atr_c2c: Vec<f64>,
    pub(super) kf_adx_slope: Vec<f64>,
    pub(super) kf_trend_momentum: Vec<f64>,
    pub(super) kf_trend_volatility_ratio: Vec<f64>,
    pub(super) kf_price_deviation: Vec<f64>,
    pub(super) kf_vs_9ema: Vec<f64>,
    pub(super) kf_vs_200sma: Vec<f64>,
    pub(super) kf_innovation_abs: Vec<f64>,
    pub(super) kf_adx_deviation: Vec<f64>,
    pub(super) kf_adx_innovation_abs: Vec<f64>,
    pub(super) kf_adx_momentum_5: Vec<f64>,
    pub(super) kf_atr_pct: Vec<f64>,
    pub(super) kf_atr_c2c_pct: Vec<f64>,
    pub(super) kf_atr_vs_c2c: Vec<f64>,
    pub(super) kf_atr_deviation: Vec<f64>,
    pub(super) kf_atr_momentum_5: Vec<f64>,
    pub(super) kf_atr_c2c_momentum_5: Vec<f64>,
    pub(super) kf_atr_innovation: Vec<f64>,
    pub(super) kf_atr_c2c_innovation: Vec<f64>,
}

impl DerivedMetrics {
    pub(in crate::engineer) fn new(prices: &PriceSeries) -> Self {
        let body = diff(&prices.close, &prices.open);
        let abs_body = body.iter().map(|v| v.abs()).collect::<Vec<_>>();
        let upper_wick = upper_wick(&prices.open, &prices.close, &prices.high);
        let lower_wick = lower_wick(&prices.open, &prices.close, &prices.low);
        let max_wick = elementwise_max(&upper_wick, &lower_wick);

        let ema9 = ema(&prices.close, 9);
        let ema20 = ema(&prices.close, 20);
        let ema50 = ema(&prices.close, 50);
        let ema200 = ema(&prices.close, 200);
        let sma200 = sma(&prices.close, 200);
        let (kf_smooth, kf_innovation) = kalman_filter(&prices.close, 0.01, 0.1);
        let (kf_trend, _) = kalman_filter(&prices.close, 0.001, 0.5);
        let kf_ma = kf_smooth.clone();
        let kf_close_momentum = derivative(&kf_smooth, 1);
        let kf_slope_5 = derivative(&kf_smooth, 5)
            .into_iter()
            .map(|slope| slope / 5.0)
            .collect::<Vec<_>>();

        let momentum_14 = momentum(&prices.close, 14);
        let roc5 = roc(&prices.close, 5);
        let roc10 = roc(&prices.close, 10);

        let adx = adx(&prices.high, &prices.low, &prices.close, 14);

        let atr = atr(&prices.high, &prices.low, &prices.close, 14);
        let atr_c2c = atr_close_to_close(&prices.close, 14);
        let atr_pct = ratio(&atr, &prices.close);
        let atr_c2c_pct = ratio(&atr_c2c, &prices.close);
        let bar_range = range(&prices.high, &prices.low);
        let bar_range_pct = ratio(&bar_range, &prices.close);
        let volatility_20_cv = rolling_coeff_var(&prices.close, 20);
        let body_size_pct = ratio(&abs_body, &prices.close);
        let upper_shadow_ratio = ratio(&upper_wick, &bar_range);
        let lower_shadow_ratio = ratio(&lower_wick, &bar_range);
        let wicks_diff = prices
            .open
            .iter()
            .zip(prices.close.iter())
            .zip(prices.low.iter())
            .zip(prices.high.iter())
            .map(|(((open, close), low), high)| {
                if close > open {
                    open - low
                } else {
                    high - open
                }
            })
            .collect::<Vec<_>>();
        let wicks_diff_sma14 = sma(&wicks_diff, 14);
        let (kf_wicks_smooth, _) = kalman_filter(&wicks_diff, 0.01, 0.1);
        let total_wick: Vec<f64> = upper_wick
            .iter()
            .zip(lower_wick.iter())
            .map(|(u, l)| u + l)
            .collect();
        let body_to_total_wick = ratio_with_eps(&abs_body, &total_wick, SMALL_DIVISOR);

        let price_vs_200sma_dev = deviation(&prices.close, &sma200);
        let price_vs_9ema_dev = deviation(&prices.close, &ema9);
        let nine_to_two_hundred = deviation(&ema9, &ema200);

        let rsi14 = rsi(&prices.close, 14, 0);
        let rsi7 = rsi(&prices.close, 7, 0);
        let rsi21 = rsi(&prices.close, 21, 0);
        let momentum_score = momentum_score(&rsi14, &roc5, &roc10);
        let atr_mean50 = sma(&atr, 50);
        let atr_c2c_mean50 = sma(&atr_c2c, 50);

        let (macd, macd_signal, macd_hist) = barsmith_indicators::macd(&prices.close, 0);
        let (stoch_k, stoch_d) = stochastic(&prices.close, &prices.high, &prices.low, 14, 3, 0);

        let (bb_mid, bb_upper, bb_lower, bb_std) = bollinger(&prices.close, 20, 2.0, 0);
        let ext = extension(&prices.high, &prices.low, 20);
        let ext_sma14 = sma(&ext, 14);

        let kf_close_minus = diff(&prices.close, &kf_smooth);
        let kf_price_deviation = ratio(&kf_close_minus, &prices.close);
        let kf_vs_9ema = ratio(&diff(&kf_smooth, &ema9), &ema9);
        let kf_vs_200sma = ratio(&diff(&kf_smooth, &sma200), &sma200);
        let kf_innovation_abs = vector_abs(&kf_innovation);

        let (kf_adx, kf_adx_innovation) = kalman_filter(&adx, 0.005, 0.2);
        let kf_adx_slope = derivative(&kf_adx, 1);
        let kf_adx_deviation = ratio_with_eps(
            &diff(&adx, &kf_adx),
            &add_scalar(&kf_adx, SMALL_DIVISOR),
            SMALL_DIVISOR,
        );
        let kf_adx_innovation_abs = vector_abs(&kf_adx_innovation);
        let kf_adx_momentum_5 = derivative(&kf_adx, 5);

        let (kf_atr, kf_atr_innovation) = kalman_filter(&atr, 0.01, 0.15);
        let (kf_atr_c2c, kf_atr_c2c_innovation) = kalman_filter(&atr_c2c, 0.01, 0.15);
        let kf_atr_pct = ratio(&kf_atr, &prices.close);
        let kf_atr_c2c_pct = ratio(&kf_atr_c2c, &prices.close);
        let kf_atr_vs_c2c = ratio_with_eps(
            &kf_atr,
            &add_scalar(&kf_atr_c2c, SMALL_DIVISOR),
            SMALL_DIVISOR,
        );
        let kf_atr_deviation = ratio_with_eps(
            &diff(&atr, &kf_atr),
            &add_scalar(&kf_atr, SMALL_DIVISOR),
            SMALL_DIVISOR,
        );
        let kf_atr_momentum_5 = derivative(&kf_atr, 5);
        let kf_atr_c2c_momentum_5 = derivative(&kf_atr_c2c, 5);
        let kf_trend_momentum = derivative(&kf_trend, 5);
        let denom_vol: Vec<f64> = kf_atr_pct
            .iter()
            .map(|v| v * 100.0 + SMALL_DIVISOR)
            .collect();
        let kf_trend_volatility_ratio = ratio_with_eps(&kf_adx, &denom_vol, SMALL_DIVISOR);
        let body_atr_ratio = ratio_with_eps(&abs_body, &atr, SMALL_DIVISOR);

        Self {
            abs_body,
            upper_wick,
            lower_wick,
            max_wick,
            body_to_total_wick,
            body_atr_ratio,
            ema9,
            ema20,
            ema50,
            sma200,
            kf_ma,
            momentum_14,
            momentum_score,
            roc5,
            roc10,
            adx,
            atr,
            atr_c2c,
            atr_pct,
            atr_c2c_pct,
            bar_range_pct,
            volatility_20_cv,
            body_size_pct,
            upper_shadow_ratio,
            lower_shadow_ratio,
            wicks_diff,
            wicks_diff_sma14,
            kf_wicks_smooth,
            price_vs_200sma_dev,
            price_vs_9ema_dev,
            nine_to_two_hundred,
            rsi14,
            rsi7,
            rsi21,
            atr_mean50,
            atr_c2c_mean50,
            macd,
            macd_signal,
            macd_hist,
            stoch_k,
            stoch_d,
            bb_mid,
            bb_upper,
            bb_lower,
            bb_std,
            ext,
            ext_sma14,
            kf_smooth,
            kf_innovation,
            kf_close_momentum,
            kf_slope_5,
            kf_trend,
            kf_adx,
            kf_atr,
            kf_atr_c2c,
            kf_adx_slope,
            kf_trend_momentum,
            kf_trend_volatility_ratio,
            kf_price_deviation,
            kf_vs_9ema,
            kf_vs_200sma,
            kf_innovation_abs,
            kf_adx_deviation,
            kf_adx_innovation_abs,
            kf_adx_momentum_5,
            kf_atr_pct,
            kf_atr_c2c_pct,
            kf_atr_vs_c2c,
            kf_atr_deviation,
            kf_atr_momentum_5,
            kf_atr_c2c_momentum_5,
            kf_atr_innovation,
            kf_atr_c2c_innovation,
        }
    }
}
