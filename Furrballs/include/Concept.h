#pragma once
#include <concepts>
#include <type_traits>
#include <chrono>

template<typename T>
concept Lockable = requires(T t) {
    { t.lock() } -> std::same_as<void>;
    { t.unlock() } -> std::same_as<void>;
};