/*****************************************************************//**
 * \file   IFactory.h
 * \brief  Factory Wrapper utility
 *
 * \author The Sphynx
 * \date   July 2024
 *********************************************************************/
#include <functional>
#include <list>
#include <memory>
#include <unordered_map>
#include <iostream>


 namespace Furrball {
     /**
      * \brief Base class for factories.
      */
      class IFactory {
      public:
          virtual ~IFactory() = default;
          virtual void* create() const = 0;
      };

      /**
       * \brief Templated derived class for specific factories.
       */
      template<class Value, class... Args>
      class Factory : public IFactory {
      public:
          Factory(std::function<Value(Args...)> func, Args... args)
              : func_(func), args_(std::make_tuple(args...)) {}

          void* create() const override {
              return createImpl(std::index_sequence_for<Args...>{});
          }

      private:
          template<std::size_t... Is>
          void* createImpl(std::index_sequence<Is...>) const {
              return new Value(func_(std::get<Is>(args_)...));
          }

          std::function<Value(Args...)> func_;
          std::tuple<Args...> args_;
      };

      /**
       * \brief Wrapper class to manage factories.
       */
      class StaticFactoryWrapper {
      public:
          template<class Value, class... Args>
          static unsigned int addFactory(std::function<Value(Args...)> func, Args... args) {
              factories_.emplace_back(std::make_unique<Factory<Value, Args...>>(func, args...));
              return factories_.size() - 1;
          }

          static void* create(unsigned int id) {
              if (id < factories_.size()) {
                  return factories_[id]->create();
              }
              return nullptr;
          }

      private:
          static std::vector<std::unique_ptr<IFactory>> factories_;
      };

      // Initialize static member
      std::vector<std::unique_ptr<IFactory>> StaticFactoryWrapper::factories_;
}
