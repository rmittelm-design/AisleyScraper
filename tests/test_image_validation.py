from __future__ import annotations

import sys

import numpy as np

from aisley_scraper import image_validation


class _FakeTensor:
    def __init__(self, values) -> None:
        self.values = np.array(values, dtype=float)

    def unsqueeze(self, dim: int):
        return _FakeTensor(np.expand_dims(self.values, axis=dim))

    def norm(self, dim: int = -1, keepdim: bool = True):
        return _FakeTensor(np.linalg.norm(self.values, axis=dim, keepdims=keepdim))

    def mean(self, dim: int = 0):
        return _FakeTensor(np.mean(self.values, axis=dim))

    @property
    def T(self):
        return _FakeTensor(self.values.T)

    def squeeze(self, dim: int = 0):
        return _FakeTensor(np.squeeze(self.values, axis=dim))

    def item(self) -> float:
        return float(np.array(self.values).item())

    def __matmul__(self, other):
        return _FakeTensor(self.values @ other.values)

    def __truediv__(self, other):
        other_values = other.values if isinstance(other, _FakeTensor) else other
        return _FakeTensor(self.values / other_values)

    def __mul__(self, other):
        other_values = other.values if isinstance(other, _FakeTensor) else other
        return _FakeTensor(self.values * other_values)

    def __rmul__(self, other):
        return self.__mul__(other)

    def __getitem__(self, index):
        return _FakeTensor(self.values[index])


class _FakeTorch:
    class no_grad:
        def __enter__(self):
            return None

        def __exit__(self, exc_type, exc, tb):
            return False

    @staticmethod
    def softmax(tensor: _FakeTensor, dim: int = -1) -> _FakeTensor:
        values = tensor.values
        exp_values = np.exp(values - np.max(values, axis=dim, keepdims=True))
        return _FakeTensor(exp_values / exp_values.sum(axis=dim, keepdims=True))

    @staticmethod
    def argmax(tensor: _FakeTensor) -> _FakeTensor:
        return _FakeTensor(np.array(np.argmax(tensor.values)))

    @staticmethod
    def stack(tensors: tuple[_FakeTensor, ...], dim: int = 0) -> _FakeTensor:
        return _FakeTensor(np.stack([tensor.values for tensor in tensors], axis=dim))


class _FakeLogitScale:
    def __init__(self, value: float) -> None:
        self._value = value

    def exp(self) -> _FakeTensor:
        return _FakeTensor(np.array(self._value, dtype=float))


class _FakeModel:
    def __init__(self, prompt_count: int, *, scale: float, image_index: int) -> None:
        self.prompt_count = prompt_count
        self.image_index = image_index
        self.logit_scale = _FakeLogitScale(scale)

    def encode_text(self, prompts) -> _FakeTensor:
        _ = prompts
        return _FakeTensor(np.eye(self.prompt_count, dtype=float))

    def encode_image(self, image_input) -> _FakeTensor:
        _ = image_input
        values = np.zeros((1, self.prompt_count), dtype=float)
        values[0, self.image_index] = 1.0
        return _FakeTensor(values)


class _FakeImage:
    def convert(self, _mode: str):
        return self


def _run_product_probability(monkeypatch, *, image_index: int, scale: float = 100.0):
    prompt_count = len(image_validation.CLIP_PRODUCT_POSITIVE_PROMPTS) + len(
        image_validation.CLIP_PRODUCT_NEGATIVE_PROMPTS
    )
    model = _FakeModel(prompt_count, scale=scale, image_index=image_index)

    monkeypatch.setitem(sys.modules, "torch", _FakeTorch)
    monkeypatch.setattr(
        image_validation,
        "_get_clip",
        lambda: (model, lambda image: _FakeTensor([1.0]), lambda prompts: prompts),
    )
    monkeypatch.setattr(image_validation, "warmup_clip", lambda strict=False: None)
    monkeypatch.setattr(image_validation, "_CLIP_TEXT_PROMPTS", None)
    monkeypatch.setattr(image_validation, "_CLIP_TEXT_FEATURES", None)

    return image_validation.product_probability_clip(_FakeImage())


def test_product_probability_clip_prefers_positive_class(monkeypatch) -> None:
    result = _run_product_probability(monkeypatch, image_index=0)

    assert result["product_prob"] > 0.99
    assert result["non_product_prob"] < 0.01
    assert result["class_probs"]["product"] == result["product_prob"]


def test_product_probability_clip_prefers_negative_class(monkeypatch) -> None:
    negative_index = len(image_validation.CLIP_PRODUCT_POSITIVE_PROMPTS)
    result = _run_product_probability(monkeypatch, image_index=negative_index)

    assert result["product_prob"] < 0.01
    assert result["non_product_prob"] > 0.99


def test_clip_logit_scale_falls_back_to_default_for_invalid_value() -> None:
    class _InvalidScaleModel:
        logit_scale = object()

    assert image_validation._clip_logit_scale(_InvalidScaleModel()) == 100.0
