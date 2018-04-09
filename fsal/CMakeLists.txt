if(USE_DBUS)
  include_directories(
    ${DBUS_INCLUDE_DIRS}
    )
endif(USE_DBUS)

include_directories("${PROJECT_SOURCE_DIR}/FSAL/FSAL_PROXYFS")

########### next target ###############

SET(fsalproxyfs_LIB_SRCS
  main.c
  export.c
  handle.c
)

add_library(fsalproxyfs MODULE ${fsalproxyfs_LIB_SRCS})
add_sanitizers(fsalproxyfs)

target_link_libraries(fsalproxyfs ${SYSTEM_LIBRARIES})

set_target_properties(fsalproxyfs PROPERTIES VERSION 4.2.0 SOVERSION 4)



########### install files ###############

install(TARGETS fsalproxyfs COMPONENT fsal DESTINATION ${FSAL_DESTINATION} )
