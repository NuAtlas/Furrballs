set(NUMATIC_SOURCES
    src/NumaticUnix.cpp
)

if(WIN32)
    set(NUMATIC_SOURCES src/NumaticWin.cpp)
endif()

add_library(Numatic STATIC ${NUMATIC_SOURCES})

target_include_directories(Numatic
PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if(NOT WIN32)
    target_link_libraries(Numatic PRIVATE numa)
endif()

