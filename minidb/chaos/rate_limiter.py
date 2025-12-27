"""
Rate limiting and backpressure mechanisms.
"""

import time
import threading
from typing import Dict, Optional
from abc import ABC, abstractmethod
from dataclasses import dataclass
from collections import deque


@dataclass
class RateLimitStats:
    """Statistics for rate limiting."""
    total_requests: int = 0
    allowed_requests: int = 0
    rejected_requests: int = 0
    current_rate: float = 0.0


class RateLimiter(ABC):
    """Abstract base class for rate limiters."""
    
    @abstractmethod
    def try_acquire(self, key: str = "default") -> bool:
        """
        Try to acquire a permit.
        
        Args:
            key: Rate limit key (for per-client limiting)
            
        Returns:
            True if request is allowed
        """
        pass
    
    @abstractmethod
    def get_stats(self) -> Dict:
        """Get rate limiter statistics."""
        pass


class TokenBucketLimiter(RateLimiter):
    """
    Token bucket rate limiter.
    
    Features:
    - Configurable bucket size (burst capacity)
    - Configurable refill rate
    - Per-key limiting
    - Thread-safe
    """
    
    def __init__(self, rate: float = 100.0, burst: int = 100):
        """
        Initialize token bucket limiter.
        
        Args:
            rate: Tokens added per second
            burst: Maximum bucket size
        """
        self.rate = rate
        self.burst = burst
        
        self._lock = threading.RLock()
        self._buckets: Dict[str, dict] = {}
        self._stats = RateLimitStats()
    
    def _get_bucket(self, key: str) -> dict:
        """Get or create bucket for key."""
        if key not in self._buckets:
            self._buckets[key] = {
                "tokens": float(self.burst),
                "last_update": time.time()
            }
        return self._buckets[key]
    
    def _refill(self, bucket: dict):
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - bucket["last_update"]
        bucket["tokens"] = min(
            self.burst,
            bucket["tokens"] + elapsed * self.rate
        )
        bucket["last_update"] = now
    
    def try_acquire(self, key: str = "default") -> bool:
        """Try to acquire a token."""
        with self._lock:
            self._stats.total_requests += 1
            
            bucket = self._get_bucket(key)
            self._refill(bucket)
            
            if bucket["tokens"] >= 1.0:
                bucket["tokens"] -= 1.0
                self._stats.allowed_requests += 1
                return True
            
            self._stats.rejected_requests += 1
            return False
    
    def get_available_tokens(self, key: str = "default") -> float:
        """Get available tokens for a key."""
        with self._lock:
            bucket = self._get_bucket(key)
            self._refill(bucket)
            return bucket["tokens"]
    
    def get_stats(self) -> Dict:
        """Get statistics."""
        with self._lock:
            return {
                "type": "token_bucket",
                "rate": self.rate,
                "burst": self.burst,
                "total_requests": self._stats.total_requests,
                "allowed": self._stats.allowed_requests,
                "rejected": self._stats.rejected_requests,
                "rejection_rate": (
                    self._stats.rejected_requests / self._stats.total_requests
                    if self._stats.total_requests > 0 else 0
                )
            }


class SlidingWindowLimiter(RateLimiter):
    """
    Sliding window rate limiter.
    
    Features:
    - Fixed window with sliding behavior
    - Accurate rate limiting
    - Per-key limiting
    """
    
    def __init__(self, max_requests: int = 100, window_seconds: float = 1.0):
        """
        Initialize sliding window limiter.
        
        Args:
            max_requests: Maximum requests per window
            window_seconds: Window duration
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        
        self._lock = threading.RLock()
        self._windows: Dict[str, deque] = {}
        self._stats = RateLimitStats()
    
    def _get_window(self, key: str) -> deque:
        """Get or create window for key."""
        if key not in self._windows:
            self._windows[key] = deque()
        return self._windows[key]
    
    def _cleanup_window(self, window: deque):
        """Remove expired entries from window."""
        cutoff = time.time() - self.window_seconds
        while window and window[0] < cutoff:
            window.popleft()
    
    def try_acquire(self, key: str = "default") -> bool:
        """Try to acquire a permit."""
        with self._lock:
            self._stats.total_requests += 1
            
            window = self._get_window(key)
            self._cleanup_window(window)
            
            if len(window) < self.max_requests:
                window.append(time.time())
                self._stats.allowed_requests += 1
                return True
            
            self._stats.rejected_requests += 1
            return False
    
    def get_current_count(self, key: str = "default") -> int:
        """Get current request count in window."""
        with self._lock:
            window = self._get_window(key)
            self._cleanup_window(window)
            return len(window)
    
    def get_stats(self) -> Dict:
        """Get statistics."""
        with self._lock:
            return {
                "type": "sliding_window",
                "max_requests": self.max_requests,
                "window_seconds": self.window_seconds,
                "total_requests": self._stats.total_requests,
                "allowed": self._stats.allowed_requests,
                "rejected": self._stats.rejected_requests,
                "rejection_rate": (
                    self._stats.rejected_requests / self._stats.total_requests
                    if self._stats.total_requests > 0 else 0
                )
            }


class AdaptiveRateLimiter(RateLimiter):
    """
    Adaptive rate limiter that adjusts based on system load.
    
    Features:
    - Automatic rate adjustment
    - Backpressure detection
    - Graceful degradation
    """
    
    def __init__(self, initial_rate: float = 100.0,
                 min_rate: float = 10.0,
                 max_rate: float = 1000.0,
                 adjustment_interval: float = 1.0):
        """
        Initialize adaptive limiter.
        
        Args:
            initial_rate: Starting rate limit
            min_rate: Minimum rate limit
            max_rate: Maximum rate limit
            adjustment_interval: How often to adjust rate
        """
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.adjustment_interval = adjustment_interval
        
        self._current_rate = initial_rate
        self._limiter = TokenBucketLimiter(rate=initial_rate, burst=int(initial_rate))
        
        self._lock = threading.RLock()
        self._last_adjustment = time.time()
        self._recent_latencies: deque = deque(maxlen=100)
        self._stats = RateLimitStats()
        
        # Thresholds
        self._latency_threshold_high = 0.1  # 100ms
        self._latency_threshold_low = 0.01  # 10ms
    
    def try_acquire(self, key: str = "default") -> bool:
        """Try to acquire a permit."""
        with self._lock:
            self._stats.total_requests += 1
            self._maybe_adjust_rate()
            
            if self._limiter.try_acquire(key):
                self._stats.allowed_requests += 1
                return True
            
            self._stats.rejected_requests += 1
            return False
    
    def record_latency(self, latency_seconds: float):
        """Record a request latency for rate adjustment."""
        with self._lock:
            self._recent_latencies.append(latency_seconds)
    
    def apply_backpressure(self, factor: float = 0.5):
        """Apply backpressure by reducing rate."""
        with self._lock:
            new_rate = max(self.min_rate, self._current_rate * factor)
            self._update_rate(new_rate)
    
    def _maybe_adjust_rate(self):
        """Adjust rate based on recent performance."""
        now = time.time()
        if now - self._last_adjustment < self.adjustment_interval:
            return
        
        self._last_adjustment = now
        
        if not self._recent_latencies:
            return
        
        avg_latency = sum(self._recent_latencies) / len(self._recent_latencies)
        
        if avg_latency > self._latency_threshold_high:
            # High latency - decrease rate
            new_rate = max(self.min_rate, self._current_rate * 0.8)
            self._update_rate(new_rate)
        elif avg_latency < self._latency_threshold_low:
            # Low latency - increase rate
            new_rate = min(self.max_rate, self._current_rate * 1.1)
            self._update_rate(new_rate)
    
    def _update_rate(self, new_rate: float):
        """Update the rate limit."""
        self._current_rate = new_rate
        self._limiter = TokenBucketLimiter(
            rate=new_rate,
            burst=int(new_rate)
        )
    
    def get_current_rate(self) -> float:
        """Get current rate limit."""
        with self._lock:
            return self._current_rate
    
    def get_stats(self) -> Dict:
        """Get statistics."""
        with self._lock:
            avg_latency = (
                sum(self._recent_latencies) / len(self._recent_latencies)
                if self._recent_latencies else 0
            )
            
            return {
                "type": "adaptive",
                "current_rate": self._current_rate,
                "min_rate": self.min_rate,
                "max_rate": self.max_rate,
                "avg_latency_ms": avg_latency * 1000,
                "total_requests": self._stats.total_requests,
                "allowed": self._stats.allowed_requests,
                "rejected": self._stats.rejected_requests,
                "rejection_rate": (
                    self._stats.rejected_requests / self._stats.total_requests
                    if self._stats.total_requests > 0 else 0
                )
            }


class BackpressureManager:
    """
    Manages backpressure across the system.
    
    Features:
    - Monitor queue depths
    - Apply graduated backpressure
    - Coordinate rate limiting
    """
    
    def __init__(self, rate_limiter: RateLimiter = None):
        self.rate_limiter = rate_limiter or TokenBucketLimiter()
        
        self._lock = threading.RLock()
        self._queue_depths: Dict[str, int] = {}
        self._backpressure_level = 0.0  # 0.0 to 1.0
        
        # Thresholds
        self._low_threshold = 100
        self._high_threshold = 1000
    
    def update_queue_depth(self, queue_name: str, depth: int):
        """Update queue depth for backpressure calculation."""
        with self._lock:
            self._queue_depths[queue_name] = depth
            self._calculate_backpressure()
    
    def _calculate_backpressure(self):
        """Calculate overall backpressure level."""
        if not self._queue_depths:
            self._backpressure_level = 0.0
            return
        
        max_depth = max(self._queue_depths.values())
        
        if max_depth <= self._low_threshold:
            self._backpressure_level = 0.0
        elif max_depth >= self._high_threshold:
            self._backpressure_level = 1.0
        else:
            # Linear interpolation
            self._backpressure_level = (
                (max_depth - self._low_threshold) /
                (self._high_threshold - self._low_threshold)
            )
        
        # Apply backpressure to adaptive limiter
        if isinstance(self.rate_limiter, AdaptiveRateLimiter):
            if self._backpressure_level > 0.5:
                self.rate_limiter.apply_backpressure(
                    1.0 - self._backpressure_level * 0.5
                )
    
    def should_accept_request(self, key: str = "default") -> bool:
        """Check if a request should be accepted."""
        # High backpressure - probabilistic rejection
        if self._backpressure_level > 0.8:
            import random
            if random.random() < self._backpressure_level - 0.5:
                return False
        
        return self.rate_limiter.try_acquire(key)
    
    def get_backpressure_level(self) -> float:
        """Get current backpressure level (0.0 to 1.0)."""
        with self._lock:
            return self._backpressure_level
    
    def get_stats(self) -> Dict:
        """Get backpressure statistics."""
        with self._lock:
            return {
                "backpressure_level": self._backpressure_level,
                "queue_depths": dict(self._queue_depths),
                "rate_limiter": self.rate_limiter.get_stats()
            }
