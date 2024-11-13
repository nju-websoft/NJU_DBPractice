find_package(BISON 3.8 REQUIRED)
find_package(FLEX 2.6 REQUIRED)

bison_target(yacc yacc.y ${CMAKE_CURRENT_SOURCE_DIR}/yacc.tab.cpp
        DEFINES_FILE ${CMAKE_CURRENT_SOURCE_DIR}/yacc.tab.h)
flex_target(lex lex.l ${CMAKE_CURRENT_SOURCE_DIR}/lex.yy.cpp)
add_flex_bison_dependency(lex yacc)

include_directories(include)

set(SOURCES ${BISON_yacc_OUTPUT_SOURCE} ${FLEX_lex_OUTPUTS} ast.cpp parser.cpp)
add_library(parser SHARED ${SOURCES})
target_link_libraries(parser fmt::fmt)