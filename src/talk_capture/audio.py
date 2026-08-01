"""Turning an aiortc audio frame into the PCM the contract specifies.

The one transform every captured frame goes through, kept pure and in one place:
mono extraction, dtype normalisation, resampling. The gateway is fed
float32 at 16 kHz, and that is fixed at the contract level so every source
produces byte-identical model input regardless of what the conference server
happened to send.
"""
from __future__ import annotations

import numpy as np

# Canonical contract rate. Not a tuning knob — the far side assumes it.
TARGET_RATE = 16000


def _frame_pts_ms(frame) -> int:
    """Media presentation timestamp of an aiortc frame in ms (track-relative,
    arbitrary origin). 0 if unavailable — the transcriber then falls back to
    recv-time for that frame's spacing."""
    try:
        pts = getattr(frame, "pts", None)
        tb = getattr(frame, "time_base", None)
        if pts is None or tb is None:
            return 0
        return int(float(pts) * float(tb) * 1000.0)
    except Exception:
        return 0


def frame_to_pcm(frame, target_rate: int) -> np.ndarray:
    """Convert an aiortc-style audio frame to mono float64 PCM at target_rate.

    PURE — no engine/session state. The single source of truth for the
    "frame -> canonical PCM" transform: mono extraction, dtype normalisation,
    and resampling (anti-aliased decimation for integer ratios, linear interp
    otherwise). Shared by the in-process engine and the gRPC source path so both
    feed the model bit-identical samples (dual-run parity, Phase 2c).

    Returns float64 (the engine casts to float32 for the model and uses the
    float64 form for RMS; the gRPC source casts to float32 for the wire). The
    transform is idempotent at target_rate: feeding an already-target-rate mono
    float frame back through returns the same samples (no second resample), so
    the source -> wire -> transcriber round trip preserves the values.

    Raises on an undecodable frame (callers handle).
    """
    audio_array = frame.to_ndarray()

    # ── Extract mono ──
    is_planar = hasattr(frame.format, "is_planar") and frame.format.is_planar
    n_channels = (
        len(frame.layout.channels) if hasattr(frame.layout, "channels") else 1
    )
    if audio_array.ndim > 1:
        if is_planar:
            audio_array = audio_array[0]
        else:
            audio_array = audio_array.flatten()
            if n_channels > 1:
                audio_array = audio_array[::n_channels]
    else:
        if n_channels > 1 and not is_planar:
            audio_array = audio_array[::n_channels]

    # ── Normalise to float64 ──
    if np.issubdtype(audio_array.dtype, np.integer):
        max_val = np.iinfo(audio_array.dtype).max
        audio_float = audio_array.astype(np.float64) / max_val
    else:
        audio_float = audio_array.astype(np.float64)

    # ── Resample if needed (aiortc typically gives 48 kHz) ──
    source_rate = frame.sample_rate
    if source_rate != target_rate and source_rate > target_rate:
        ratio = source_rate / target_rate
        int_ratio = int(ratio)
        if int_ratio == ratio and int_ratio > 1:
            audio_float = _decimate(audio_float, int_ratio)
        else:
            n_out = int(len(audio_float) / ratio)
            indices = np.linspace(0, len(audio_float) - 1, n_out)
            audio_float = np.interp(
                indices, np.arange(len(audio_float)), audio_float
            )

    return audio_float


def _decimate(audio: np.ndarray, factor: int) -> np.ndarray:
    """Anti-aliased decimation using a simple FIR low-pass filter.

    Applies a windowed-sinc low-pass filter at cutoff = 1/(2*factor)
    before downsampling to prevent aliasing.
    """
    if factor <= 1:
        return audio

    # Design a simple low-pass FIR filter
    n_taps = max(factor * 8, 31)
    if n_taps % 2 == 0:
        n_taps += 1

    # Windowed-sinc low-pass filter
    cutoff = 1.0 / factor
    half = n_taps // 2
    t = np.arange(-half, half + 1, dtype=np.float64)

    with np.errstate(divide="ignore", invalid="ignore"):
        h = np.where(t == 0, cutoff, np.sin(np.pi * cutoff * t) / (np.pi * t))

    # Hamming window
    window = 0.54 - 0.46 * np.cos(2.0 * np.pi * np.arange(n_taps) / (n_taps - 1))
    h = h * window

    # Normalize
    h = h / np.sum(h)

    # Apply filter (convolve) then decimate
    filtered = np.convolve(audio, h, mode="same")
    return filtered[::factor]
