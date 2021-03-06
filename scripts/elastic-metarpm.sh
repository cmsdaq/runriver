#!/bin/bash -e

#supported python 3.4 (or higher)
pythonlink="python3.4"

while ! [ "$pythonlink" = "" ]
do
  pythonlinklast=$pythonlink
  readlink /usr/bin/$pythonlink > pytmp | true
  pythonlink=`cat pytmp`
  rm -rf pytmp
  #echo "running readlink /usr/bin/$pythonlinklast --> /usr/bin/$pythonlink"
done
pythonlinklast=`basename $pythonlinklast`
echo "will compile packages for: $pythonlinklast"
pyexec=$pythonlinklast
python_dir=$pythonlinklast
python_version=${python_dir:6}

BUILD_ARCH=noarch
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SCRIPTDIR/..
BASEDIR=$PWD

PACKAGENAME="fffmeta-elastic"

# create a build area

SCRATCHDIR=/tmp/$PACKAGENAME-scratch-tmp

echo "removing old build area"
rm -rf /tmp/$PACKAGENAME-build-tmp
rm -rf $SCRATCHDIR
echo "creating new build area"
mkdir  /tmp/$PACKAGENAME-build-tmp
mkdir  $SCRATCHDIR
ls
cd     /tmp/$PACKAGENAME-build-tmp
mkdir BUILD
mkdir RPMS
TOPDIR=$PWD
echo "working in $PWD"
ls

#pluginpath="/opt/fff/esplugins/"
#pluginname1="license"

jarversion=`$SCRIPTDIR/getpom.py $SCRIPTDIR/../pom.xml`
#riverfile="river-runriver-$jarversion-jar-with-dependencies.jar"
riverfile="river-runriver-$jarversion.jar"

if [ ! -f $SCRIPTDIR/../target/$riverfile ]; then
 echo "missing river file $SCRIPTDIR/../target/$riverfile"
 exit 1
fi

cp -r $BASEDIR/libpython $SCRATCHDIR/
echo "Moving files to their compile scratch area"

#python-prctl
cd $SCRATCHDIR/libpython/python-prctl
$pyexec ./setup.py -q build
$pyexec - <<EOF
import py_compile
py_compile.compile("build/lib.linux-x86_64-${python_version}/prctl.py")
EOF
$pyexec -O - <<EOF
import py_compile
py_compile.compile("build/lib.linux-x86_64-${python_version}/prctl.py")
EOF
mkdir -p $SCRATCHDIR/usr/lib64/$python_dir/site-packages
cp -R build/lib.linux-x86_64-${python_version}/*prctl* ${SCRATCHDIR}/usr/lib64/$python_dir/site-packages/
cat > $SCRATCHDIR/usr/lib64/${python_dir}/site-packages/python_prctl-1.5.0-py${python_version}.egg-info <<EOF
Metadata-Version: 1.0
Name: python-prctl
Version: 1.5.0
Summary: Python(ic) interface to the linux prctl syscall
Home-page: http://github.com/seveas/python-prctl
Author: Dennis Kaarsemaker
Author-email: dennis@kaarsemaker.net
License: UNKNOWN
Description: UNKNOWN
Platform: UNKNOWN
Classifier: Development Status :: 5 - Production/Stable
Classifier: Intended Audience :: Developers
Classifier: License :: OSI Approved :: GNU General Public License (GPL)
Classifier: Operating System :: POSIX :: Linux
Classifier: Programming Language :: C
Classifier: Programming Language :: Python
Classifier: Topic :: Security
EOF

cd $TOPDIR
# we are done here, write the specs and make the fu***** rpm
cat > fffmeta-elastic.spec <<EOF
Name: $PACKAGENAME
Version: 2.8.5
Release: 0
Summary: hlt daemon
License: gpl
Group: DAQ
Packager: smorovic
Source: none
%define __jar_repack %{nil}
%define _topdir $TOPDIR
BuildArch: $BUILD_ARCH
AutoReqProv: no
#Requires:elasticsearch => 7.2.0, cx_Oracle >= 5.1.2, java-1.8.0-oracle-headless >= 1.8.0.45, php >= 5.3.3, php-oci8 >= 1.4.9
Requires:elasticsearch => 7.2.0, java-1.8.0-oracle-headless >= 1.8.0.45, php >= 5.3.3, php-oci8 >= 1.4.9, python34, python34-requests

Provides:/opt/fff/configurefff.sh
Provides:/opt/fff/essetupmachine.py
Provides:/opt/fff/init.d/fff-config
Provides:/opt/fff/riverMapping.py
Provides:/opt/fff/insertRiver.py
Provides:/opt/fff/setup_river_index.py
Provides:/opt/fff/updatemappings.py
Provides:/opt/fff/river-daemon.py
Provides:/opt/fff/demote.py
Provides:/opt/fff/log4j2.properties
Provides:/opt/fff/init.d/riverd
Provides:/opt/fff/$riverfile
Provides:/opt/fff/tools
Provides:/opt/fff/tools,mapper
Provides:/opt/fff/tools/mapper/*
Provides:/etc/rsyslog.d/48-river.conf
Provides:/etc/logrotate.d/river
Provides:/etc/elasticsearch/users.f3
Provides:/etc/elasticsearch/users_roles.f3
Provides:/etc/elasticsearch/roles.yml.f3
Provides:/etc/elasticsearch/certs/elastic-certificates.p12
Provides:/usr/lib/systemd/system/fff.service
Provides:/usr/lib/systemd/system/riverd.service
Provides:/usr/lib64/$python_dir/site-packages/*prctl*
Provides:/usr/lib64/$python_dir/site-packages/__pycache__/prctl.*
Provides:/usr/lib64/$python_dir/site-packages/python_prctl-1.5.0-py${python_version}.egg-info

%description
fffmeta configuration setup package

%prep
%build

%install
rm -rf \$RPM_BUILD_ROOT
mkdir -p \$RPM_BUILD_ROOT
%__install -d "%{buildroot}/usr/lib64/$python_dir/site-packages"
%__install -d "%{buildroot}/opt/fff"
%__install -d "%{buildroot}/opt/fff/backup"
%__install -d "%{buildroot}/opt/fff/esplugins"
%__install -d "%{buildroot}/opt/fff/init.d"
%__install -d "%{buildroot}/opt/fff/tools"
%__install -d "%{buildroot}/opt/fff/tools/mapper"
%__install -d "%{buildroot}/usr/lib/systemd/system"
%__install -d "%{buildroot}/etc/rsyslog.d"
%__install -d "%{buildroot}/etc/logrotate.d"
%__install -d "%{buildroot}/var/log/river"
%__install -d "%{buildroot}/etc/elasticsearch"
%__install -d "%{buildroot}/etc/elasticsearch/certs"

cp $SCRATCHDIR/usr/lib64/$python_dir/site-packages/*prctl* %{buildroot}/usr/lib64/$python_dir/site-packages/

cp $BASEDIR/etc/rsyslog.d/48-river.conf %{buildroot}/etc/rsyslog.d/48-river.conf
cp $BASEDIR/etc/logrotate.d/river %{buildroot}/etc/logrotate.d/river
cp $BASEDIR/python/log4j2.properties %{buildroot}/opt/fff/log4j2.properties
cp $BASEDIR/etc/elasticsearch/users.f3       %{buildroot}/etc/elasticsearch/users.f3
cp $BASEDIR/etc/elasticsearch/users_roles.f3 %{buildroot}/etc/elasticsearch/users_roles.f3
cp $BASEDIR/etc/elasticsearch/roles.yml.f3   %{buildroot}/etc/elasticsearch/roles.yml.f3
cp $BASEDIR/etc/elasticsearch/certs/elastic-certificates.p12 %{buildroot}/etc/elasticsearch/certs/elastic-certificates.p12

cp $BASEDIR/python/*.py %{buildroot}/opt/fff/

cp $BASEDIR/init.d/fff-config %{buildroot}/opt/fff/init.d/fff-config
cp $BASEDIR/init.d/*.service %{buildroot}/usr/lib/systemd/system/

echo "#!/bin/bash" > %{buildroot}/opt/fff/configurefff.sh
echo python3.4 /opt/fff/essetupmachine.py >> %{buildroot}/opt/fff/configurefff.sh

cp $BASEDIR/target/$riverfile %{buildroot}/opt/fff/$riverfile
cp $BASEDIR/esplugins/install.sh %{buildroot}/opt/fff/esplugins/install.sh
cp $BASEDIR/esplugins/uninstall.sh %{buildroot}/opt/fff/esplugins/uninstall.sh

cp -R $BASEDIR/scripts/tools/mapper/{*.py,*.sh,*.md} %{buildroot}/opt/fff/tools/mapper/

%files
%defattr(-, root, root, -)
#/opt/fff
%attr( 755 ,root, root) /var/log/river
%attr( 755 ,root, root) /opt/fff/essetupmachine.py
%attr( 755 ,root, root) /opt/fff/essetupmachine.pyc
%attr( 755 ,root, root) /opt/fff/essetupmachine.pyo
%attr( 755 ,root, root) /opt/fff/demote.py
%attr( 755 ,root, root) /opt/fff/demote.pyc
%attr( 755 ,root, root) /opt/fff/demote.pyo
%attr( 755 ,root, root) /opt/fff/riverMapping.py
%attr( 755 ,root, root) /opt/fff/riverMapping.pyc
%attr( 755 ,root, root) /opt/fff/riverMapping.pyo
%attr( 755 ,root, root) /opt/fff/insertRiver.py
%attr( 755 ,root, root) /opt/fff/insertRiver.pyc
%attr( 755 ,root, root) /opt/fff/insertRiver.pyo
%attr( 755 ,root, root) /opt/fff/setup_river_index.py
%attr( 755 ,root, root) /opt/fff/setup_river_index.pyo
%attr( 755 ,root, root) /opt/fff/setup_river_index.pyc
%attr( 755 ,root, root) /opt/fff/updatemappings.py
%attr( 755 ,root, root) /opt/fff/updatemappings.pyo
%attr( 755 ,root, root) /opt/fff/updatemappings.pyc
%attr( 755 ,root, root) /opt/fff/river-daemon.py
%attr( 755 ,root, root) /opt/fff/river-daemon.pyc
%attr( 755 ,root, root) /opt/fff/river-daemon.pyo
%attr( 755 ,root, root) /opt/fff/log4j2.properties
%attr( 700 ,root, root) /opt/fff/configurefff.sh
%attr( 755 ,root, root) /opt/fff/init.d/fff-config
%attr( 755 ,root, root) /usr/lib/systemd/system/fff.service
%attr( 755 ,root, root) /usr/lib/systemd/system/riverd.service
%attr( 755 ,root, root) /opt/fff/esplugins/install.sh
%attr( 755 ,root, root) /opt/fff/esplugins/uninstall.sh
%attr( 755 ,root, root) /opt/fff/$riverfile
%attr( 644 ,root, root) /etc/rsyslog.d/48-river.conf
%attr( 644 ,root, root) /etc/logrotate.d/river
%attr( 400 ,elasticsearch, elasticsearch) /etc/elasticsearch/users.f3
%attr( 664 ,elasticsearch, elasticsearch) /etc/elasticsearch/users_roles.f3
%attr( 664 ,elasticsearch, elasticsearch) /etc/elasticsearch/roles.yml.f3
%attr( 400 ,elasticsearch, elasticsearch) /etc/elasticsearch/certs/elastic-certificates.p12
%attr( 755 ,root, root) /opt/fff/tools/mapper/*.py
%attr( 755 ,root, root) /opt/fff/tools/mapper/*.sh
%attr( 644 ,root, root) /opt/fff/tools/mapper/*.pyc
%attr( 644 ,root, root) /opt/fff/tools/mapper/*.pyo
%attr( 644 ,root, root) /opt/fff/tools/mapper/*.md
%attr( 755 ,root, root) /usr/lib64/${python_dir}/site-packages/*prctl*
%attr( 755 ,root, root) /usr/lib64/${python_dir}/site-packages/__pycache__/prctl*

%post
#echo "post install trigger"
echo "making symbolic links"
ln -s -f river.jar /opt/fff/river_dv.jar
ln -s -f $riverfile /opt/fff/river.jar

%triggerin -- elasticsearch
#echo "triggered on elasticsearch update or install as well as this rpm update"

$pyexec /opt/fff/essetupmachine.py restore
$pyexec /opt/fff/essetupmachine.py

#update permissions in case new rpm changed uid/guid
chown -R elasticsearch:elasticsearch /var/log/elasticsearch
chown -R elasticsearch:elasticsearch /elasticsearch/lib/elasticsearch

#chown elasticsearch:elasticsearch /etc/elasticsearch/roles.yml* /etc/elasticsearch/users*
#chown elasticsearch:elasticsearch /etc/elasticsearch/certs/elastic-certificates.p12
chmod a+xr -R /etc/elasticsearch
chmod a+r -R /var/log/elasticsearch
chmod 400 /etc/elasticsearch/users.f3 /etc/elasticsearch/users

##echo "Installing plugins..."
##/opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname1 > /dev/null

#restart (should be re-enabled)

if [ -d /elasticsearch ]
then
  if [ ! -d /elasticsearch/lib/elasticsearch ]
  then
      mkdir -p /elasticsearch/lib/elasticsearch
      chown -R elasticsearch:elasticsearch /elasticsearch/lib/elasticsearch
  fi
fi
        
/usr/bin/systemctl enable elasticsearch

/usr/bin/systemctl reenable riverd
/usr/bin/systemctl reenable fff
/usr/bin/systemctl daemon-reload

/usr/bin/systemctl reenable riverd
/usr/bin/systemctl reenable fff

/usr/bin/systemctl restart riverd


%preun

if [ \$1 == 0 ]; then 

  /usr/bin/systemctl disable fff
  /usr/bin/systemctl stop riverd
  /usr/bin/systemctl disable riverd
  /usr/bin/systemctl disable elasticsearch

  #delete symbolic links
  rm -rf /opt/fff/river_dv.jar /opt/fff/river.jar

  $pyexec /opt/fff/essetupmachine.py restore
fi

#%verifyscript

EOF

rpmbuild --target noarch --define "_topdir `pwd`/RPMBUILD" -bb fffmeta-elastic.spec

