cmake_minimum_required(VERSION 3.2)

if ($ENV{TRAVIS_BUILD_NUMBER})
    project(bulkmt VERSION 0.0.$ENV{TRAVIS_BUILD_NUMBER})
else()
    project(bulkmt VERSION 0.0.13)
endif()

add_executable(bulkmt main.cpp)

set_target_properties(bulkmt PROPERTIES
    CXX_STANDARD 14
    CXX_STANDARD_REQUIRED ON
    #COMPILE_OPTIONS "-Wpedantic;-Wall;-Wextra;-fsanitize=thread"   #oh nonono
    COMPILE_OPTIONS "-Wpedantic;-Wall;-Wextra;"
    LINK_OPTIONS "-fsanitize=thread"
)

set_target_properties(bulkmt PROPERTIES LINK_FLAGS -pthread
)

install(TARGETS bulkmt RUNTIME DESTINATION bin)

set(CPACK_GENERATOR DEB)

set(CPACK_PACKAGE_VERSION_MAJOR "${PROJECT_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${PROJECT_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${PROJECT_VERSION_PATCH}")

set(CPACK_PACKAGE_CONTACT r.muzyukin@gmail.com)

include(CPack)
