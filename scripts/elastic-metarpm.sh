#!/bin/bash -e

alias python=`readlink /usr/bin/python2`
#python_dir=`readlink /usr/bin/python2`
python_dir=`python -c 'import platform; print("python"+platform.python_version_tuple()[0]+"."+platform.python_version_tuple()[1])'`
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

cd $SCRATCHDIR/libpython/python-prctl
#python-prctl
./setup.py -q build
python - <<EOF
import py_compile
py_compile.compile("build/lib.linux-x86_64-${python_version}/prctl.py")
EOF
python -O - <<EOF
import py_compile
py_compile.compile("build/lib.linux-x86_64-${python_version}/prctl.py")
EOF
mkdir -p $SCRATCHDIR/usr/lib64/$python_dir/site-packages
cp build/lib.linux-x86_64-${python_version}/prctl.pyo $SCRATCHDIR/usr/lib64/${python_dir}/site-packages/
cp build/lib.linux-x86_64-${python_version}/prctl.py  $SCRATCHDIR/usr/lib64/${python_dir}/site-packages/
cp build/lib.linux-x86_64-${python_version}/prctl.pyc $SCRATCHDIR/usr/lib64/${python_dir}/site-packages/
cp build/lib.linux-x86_64-${python_version}/_prctl.so $SCRATCHDIR/usr/lib64/${python_dir}/site-packages/
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
Version: 2.7.2
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
Requires:elasticsearch => 7.0.1, cx_Oracle >= 5.1.2, java-1.8.0-oracle-headless >= 1.8.0.45, php >= 5.3.3, php-oci8 >= 1.4.9

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
Provides:/etc/rsyslog.d/48-river.conf
Provides:/etc/logrotate.d/river
Provides:/usr/lib/systemd/system/fff.service
Provides:/usr/lib/systemd/system/riverd.service
Provides:/usr/lib64/$python_dir/site-packages/prctl.py
Provides:/usr/lib64/$python_dir/site-packages/prctl.pyc
Provides:/usr/lib64/$python_dir/site-packages/_prctl.so
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
%__install -d "%{buildroot}/usr/lib/systemd/system"
%__install -d "%{buildroot}/etc/rsyslog.d"
%__install -d "%{buildroot}/etc/logrotate.d"
%__install -d "%{buildroot}/var/log/river"

cp $SCRATCHDIR/usr/lib64/$python_dir/site-packages/prctl.py %{buildroot}/usr/lib64/$python_dir/site-packages/
cp $SCRATCHDIR/usr/lib64/$python_dir/site-packages/prctl.pyc %{buildroot}/usr/lib64/$python_dir/site-packages/
cp $SCRATCHDIR/usr/lib64/$python_dir/site-packages/prctl.pyo %{buildroot}/usr/lib64/$python_dir/site-packages/
cp $SCRATCHDIR/usr/lib64/$python_dir/site-packages/_prctl.so %{buildroot}/usr/lib64/$python_dir/site-packages/
cp $SCRATCHDIR/usr/lib64/$python_dir/site-packages/python_prctl-1.5.0-py${python_version}.egg-info  %{buildroot}/usr/lib64/$python_dir/site-packages/

cp $BASEDIR/etc/rsyslog.d/48-river.conf %{buildroot}/etc/rsyslog.d/48-river.conf
cp $BASEDIR/etc/logrotate.d/river %{buildroot}/etc/logrotate.d/river
cp $BASEDIR/python/log4j2.properties %{buildroot}/opt/fff/log4j2.properties

cp $BASEDIR/python/*.py %{buildroot}/opt/fff/

cp $BASEDIR/init.d/fff-config %{buildroot}/opt/fff/init.d/fff-config
cp $BASEDIR/init.d/*.service %{buildroot}/usr/lib/systemd/system/

echo "#!/bin/bash" > %{buildroot}/opt/fff/configurefff.sh
echo python2 /opt/fff/essetupmachine.py >> %{buildroot}/opt/fff/configurefff.sh

cp $BASEDIR/target/$riverfile %{buildroot}/opt/fff/$riverfile
cp $BASEDIR/esplugins/install.sh %{buildroot}/opt/fff/esplugins/install.sh
cp $BASEDIR/esplugins/uninstall.sh %{buildroot}/opt/fff/esplugins/uninstall.sh

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
%attr( 755 ,root, root) /usr/lib64/$python_dir/site-packages/prctl.py
%attr( 755 ,root, root) /usr/lib64/$python_dir/site-packages/prctl.pyo
%attr( 755 ,root, root) /usr/lib64/$python_dir/site-packages/prctl.pyc
%attr( 755 ,root, root) /usr/lib64/$python_dir/site-packages/_prctl.so
%attr( 755 ,root, root) /usr/lib64/$python_dir/site-packages/python_prctl-1.5.0-py${python_version}.egg-info

%post
#echo "post install trigger"
echo "making symbolic links"
ln -s -f river.jar /opt/fff/river_dv.jar
ln -s -f $riverfile /opt/fff/river.jar

%triggerin -- elasticsearch
#echo "triggered on elasticsearch update or install as well as this rpm update"

python2 /opt/fff/essetupmachine.py restore
python2 /opt/fff/essetupmachine.py

#update permissions in case new rpm changed uid/guid
chown -R elasticsearch:elasticsearch /var/log/elasticsearch
chown -R elasticsearch:elasticsearch /elasticsearch/lib/elasticsearch
chmod a+r -R /etc/elasticsearch
chmod a+r -R /var/log/elasticsearch

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

  python2 /opt/fff/essetupmachine.py restore
fi

#%verifyscript

EOF

rpmbuild --target noarch --define "_topdir `pwd`/RPMBUILD" -bb fffmeta-elastic.spec

