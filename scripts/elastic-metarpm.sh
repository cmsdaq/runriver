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

pluginpath="/opt/fff/esplugins/"
pluginname1="bigdesk"
pluginfile1="bigdesk-505b32e-mod2.zip"
pluginname2="head"
pluginfile2="head-master.zip"
pluginname3="kopf"
pluginfile3="elasticsearch-kopf-2.1.1.zip"
pluginname4="hq"
pluginfile4="hq-v2.0.3.zip"
pluginname5="delete-by-query"
pluginfile5="delete-by-query-2.2.0.zip"


riverfile="river-runriver-1.6.1-jar-with-dependencies.jar"

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
Version: 2.2.0
Release: 0es512
Summary: hlt daemon
License: gpl
Group: DAQ
Packager: smorovic
Source: none
%define __jar_repack %{nil}
%define _topdir $TOPDIR
BuildArch: $BUILD_ARCH
AutoReqProv: no
Requires:elasticsearch => 5.1, cx_Oracle >= 5.1.2, java-1.8.0-oracle-headless >= 1.8.0.45, php >= 5.3.3, php-oci8 >= 1.4.9

Provides:/opt/fff/configurefff.sh
Provides:/opt/fff/essetupmachine.py
Provides:/etc/init.d/fffmeta
Provides:/etc/init.d/fff-es
Provides:/opt/fff/daemon2.py
Provides:/opt/fff/river-daemon.py
Provides:/etc/init.d/riverd
Provides:/opt/fff/$riverfile
Provides:/etc/rsyslog.d/48-river.conf
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
%__install -d "%{buildroot}/etc/init.d"
%__install -d "%{buildroot}/etc/rsyslog.d"

mkdir -p usr/lib64/$python_dir/site-packages
mkdir -p opt/fff/esplugins
mkdir -p opt/fff/backup
mkdir -p etc/init.d/
mkdir -p etc/rsyslog.d

cp $SCRATCHDIR/usr/lib64/$python_dir/site-packages/prctl.py %{buildroot}/usr/lib64/$python_dir/site-packages/
cp $SCRATCHDIR/usr/lib64/$python_dir/site-packages/prctl.pyc %{buildroot}/usr/lib64/$python_dir/site-packages/
cp $SCRATCHDIR/usr/lib64/$python_dir/site-packages/prctl.pyo %{buildroot}/usr/lib64/$python_dir/site-packages/
cp $SCRATCHDIR/usr/lib64/$python_dir/site-packages/_prctl.so %{buildroot}/usr/lib64/$python_dir/site-packages/
cp $SCRATCHDIR/usr/lib64/$python_dir/site-packages/python_prctl-1.5.0-py${python_version}.egg-info  %{buildroot}/usr/lib64/$python_dir/site-packages/

cp $BASEDIR/etc/rsyslog.d/48-river.conf %{buildroot}/etc/rsyslog.d/48-river.conf
cp $BASEDIR/python/essetupmachine.py %{buildroot}/opt/fff/essetupmachine.py
cp $BASEDIR/python/daemon2.py %{buildroot}/opt/fff/daemon2.py
cp $BASEDIR/python/river-daemon.py %{buildroot}/opt/fff/river-daemon.py
cp $BASEDIR/python/riverd %{buildroot}/etc/init.d/riverd

echo "#!/bin/bash" > %{buildroot}/opt/fff/configurefff.sh
echo python2 /opt/fff/essetupmachine.py >> %{buildroot}/opt/fff/configurefff.sh

cp $BASEDIR/target/$riverfile %{buildroot}/opt/fff/$riverfile
cp $BASEDIR/esplugins/$pluginfile1 %{buildroot}/opt/fff/esplugins/$pluginfile1
cp $BASEDIR/esplugins/$pluginfile2 %{buildroot}/opt/fff/esplugins/$pluginfile2
cp $BASEDIR/esplugins/$pluginfile3 %{buildroot}/opt/fff/esplugins/$pluginfile3
cp $BASEDIR/esplugins/$pluginfile4 %{buildroot}/opt/fff/esplugins/$pluginfile4
cp $BASEDIR/esplugins/$pluginfile5 %{buildroot}/opt/fff/esplugins/$pluginfile5
cp $BASEDIR/esplugins/install.sh %{buildroot}/opt/fff/esplugins/install.sh
cp $BASEDIR/esplugins/uninstall.sh %{buildroot}/opt/fff/esplugins/uninstall.sh
cp $BASEDIR/scripts/fff-es %{buildroot}/etc/init.d/fff-es

echo "#!/bin/bash"                       >> %{buildroot}/etc/init.d/fffmeta
echo "#"                                 >> %{buildroot}/etc/init.d/fffmeta
echo "# chkconfig:   2345 79 22"         >> %{buildroot}/etc/init.d/fffmeta
echo "#"                                 >> %{buildroot}/etc/init.d/fffmeta
echo "if [ \\\$1 == \"start\" ]; then"   >> %{buildroot}/etc/init.d/fffmeta
echo "  /opt/fff/configurefff.sh"  >> %{buildroot}/etc/init.d/fffmeta
echo "  exit 0"                          >> %{buildroot}/etc/init.d/fffmeta
echo "fi"                                >> %{buildroot}/etc/init.d/fffmeta
echo "if [ \\\$1 == \"restart\" ]; then" >> %{buildroot}/etc/init.d/fffmeta
echo "/opt/fff/configurefff.sh"    >> %{buildroot}/etc/init.d/fffmeta
echo "  exit 0"                          >> %{buildroot}/etc/init.d/fffmeta
echo "fi"                                >> %{buildroot}/etc/init.d/fffmeta
echo "if [ \\\$1 == \"status\" ]; then"  >> %{buildroot}/etc/init.d/fffmeta
echo "echo fffmeta does not have status" >> %{buildroot}/etc/init.d/fffmeta
echo "  exit 0"                          >> %{buildroot}/etc/init.d/fffmeta
echo "fi"                                >> %{buildroot}/etc/init.d/fffmeta


%files
%defattr(-, root, root, -)
#/opt/fff
%attr( 755 ,root, root) /opt/fff/essetupmachine.py
%attr( 755 ,root, root) /opt/fff/essetupmachine.pyc
%attr( 755 ,root, root) /opt/fff/essetupmachine.pyo
%attr( 755 ,root, root) /opt/fff/daemon2.py
%attr( 755 ,root, root) /opt/fff/daemon2.pyc
%attr( 755 ,root, root) /opt/fff/daemon2.pyo
%attr( 755 ,root, root) /opt/fff/river-daemon.py
%attr( 755 ,root, root) /opt/fff/river-daemon.pyc
%attr( 755 ,root, root) /opt/fff/river-daemon.pyo
%attr( 700 ,root, root) /opt/fff/configurefff.sh
%attr( 755 ,root, root) /etc/init.d/fffmeta
%attr( 755 ,root, root) /etc/init.d/fff-es
%attr( 755 ,root, root) /etc/init.d/riverd
%attr( 444 ,root, root) /opt/fff/esplugins/$pluginfile1
%attr( 444 ,root, root) /opt/fff/esplugins/$pluginfile2
%attr( 444 ,root, root) /opt/fff/esplugins/$pluginfile3
%attr( 444 ,root, root) /opt/fff/esplugins/$pluginfile4
%attr( 444 ,root, root) /opt/fff/esplugins/$pluginfile5
%attr( 755 ,root, root) /opt/fff/esplugins/install.sh
%attr( 755 ,root, root) /opt/fff/esplugins/uninstall.sh
%attr( 755 ,root, root) /opt/fff/$riverfile
%attr( 444 ,root, root) /etc/rsyslog.d/48-river.conf
%attr( 755 ,root, root) /usr/lib64/$python_dir/site-packages/prctl.py
%attr( 755 ,root, root) /usr/lib64/$python_dir/site-packages/prctl.pyo
%attr( 755 ,root, root) /usr/lib64/$python_dir/site-packages/prctl.pyc
%attr( 755 ,root, root) /usr/lib64/$python_dir/site-packages/_prctl.so
%attr( 755 ,root, root) /usr/lib64/$python_dir/site-packages/python_prctl-1.5.0-py${python_version}.egg-info

%post
#echo "post install trigger"
chkconfig --del fffmeta
chkconfig --add fffmeta
chkconfig --del riverd
chkconfig --add riverd

#make symbolic links
ln -s -f river.jar /opt/fff/river_dv.jar
ln -s -f $riverfile /opt/fff/river.jar


#disabled, can be run manually for now

%triggerin -- elasticsearch
#echo "triggered on elasticsearch update or install"
#/sbin/service elasticsearch stop
python2 /opt/fff/essetupmachine.py restore
python2 /opt/fff/essetupmachine.py
#update permissions in case new rpm changed uid/guid
chown -R elasticsearch:elasticsearch /var/log/elasticsearch
chown -R elasticsearch:elasticsearch /var/lib/elasticsearch
chmod a+r -R /etc/elasticsearch
echo "Installing plugins..."
/opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname1 > /dev/null
#/opt/fff/esplugins/install.sh /usr/share/elasticsearch $pluginfile1 $pluginname1
/opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname2 > /dev/null
#/opt/fff/esplugins/install.sh /usr/share/elasticsearch $pluginfile2 $pluginname2
/opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname3 > /dev/null
#/opt/fff/esplugins/install.sh /usr/share/elasticsearch $pluginfile3 $pluginname3
/opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname4 > /dev/null
#/opt/fff/esplugins/install.sh /usr/share/elasticsearch $pluginfile4 $pluginname4
/opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname5 > /dev/null
#/opt/fff/esplugins/install.sh /usr/share/elasticsearch $pluginfile5 $pluginname5
/opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch elasticsearch-migration || true #remove on upgrade

chkconfig --del elasticsearch
chkconfig --add elasticsearch
#restart (should be re-enabled)
if [ -d /elasticsearch ]
then
  if [ ! -d /elasticsearch/lib/elasticsearch ]
  then
      mkdir -p /elasticsearch/lib/elasticsearch
      chown -R elasticsearch:elasticsearch /elasticsearch/lib/elasticsearch
  fi
fi
        
#/sbin/service elasticsearch start
/etc/init.d/riverd restart

%preun

if [ \$1 == 0 ]; then 

  chkconfig --del fffmeta
  chkconfig --del riverd
  chkconfig --del elasticsearch
  /sbin/service riverd stop || true
  #delete symbolic links
  rm -rf /opt/fff/river_dv.jar /opt/fff/river.jar


  #/sbin/service elasticsearch stop || true
  /opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname1 || true
  /opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname2 || true
  /opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname3 || true
  /opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname4 || true
  /opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch $pluginname5 || true
  /opt/fff/esplugins/uninstall.sh /usr/share/elasticsearch elasticsearch-migration || true


  python2 /opt/fff/essetupmachine.py restore
fi

#%verifyscript

EOF

rpmbuild --target noarch --define "_topdir `pwd`/RPMBUILD" -bb fffmeta-elastic.spec

