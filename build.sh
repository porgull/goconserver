#!/bin/bash

function check_linux_distro()
{
    local distro="$(source /etc/os-release >/dev/null 2>&1 && echo "${ID}")"
    [[ -z "${distro}" && -f /etc/redhat-release ]] && distro="rhel"
    [[ -z "${distro}" && -f /etc/SuSE-release ]] && distro="sles"
    echo "${distro}"
}

pkgname="goconserver"

build_dir=${DEST:-/${pkgname}_build}
mkdir -p $build_dir

XCAT_BUILD_DISTRO="$(check_linux_distro)"
echo "[INFO] Start to build $pkgname on $XCAT_BUILD_DISTRO"

cd "$(dirname "$0")"

case "${XCAT_BUILD_DISTRO}" in
"centos"|"fedora"|"rhel"|"sles")
    pkgtype="rpm"
    ;;
"ubuntu")
    pkgtype="deb"
    ;;
*)
    echo "[ERROR] ${XCAT_BUILD_DISTRO}: unsupported Linux distribution to build $pkgname"
    exit 1
    ;;
esac

make deps
if [ $? != 0 ]; then
    echo "[ERROR] Failed to run make deps"
    exit 1
fi

make $pkgtype |& tee /tmp/build.log
if [ $? != 0 ]; then
    echo "[ERROR] Failed to run make $pkgtype"
    exit 1
fi

buildpath=`find . -name "${pkgname}*.$pkgtype" | xargs ls -t | head -n 1`
if [ -z "$buildpath" ]; then
    echo "[ERROR] Could not find build ${pkgname}*.$pkgtype"
    exit 1
fi

cp -f $buildpath $build_dir
if [ $? != 0 ]; then
    echo "[ERROR] Failed to copy $buildpath to $build_dir"
    exit 1
fi

cp -f /tmp/build.log $build_dir

buildname=$(basename $buildpath)
echo "[INFO] Package path is $build_dir/$buildname"

exit 0
