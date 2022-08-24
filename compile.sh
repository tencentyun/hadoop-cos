#!/bin/sh

base_dir=$(cd `dirname $0`;pwd)
cd ${base_dir}
hadoop_version_array=("2.7.5" "2.8.5" "3.1.0" "3.3.0")

origin_version=$(mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive exec:exec)

for hadoop_version in ${hadoop_version_array[@]}
do
    sed -i -E "s/<hadoop\.version>.*<\/hadoop\.version>/<hadoop\.version>${hadoop_version}<\/hadoop\.version>/g" pom.xml
    mvn versions:set -DnewVersion=${hadoop_version}-${origin_version}
    mvn clean verify
    rm -rf dep/${hadoop_version}
    mkdir -p dep/${hadoop_version}
    cp target/*.jar dep/${hadoop_version}/
    cp target/*.asc dep/${hadoop_version}/
    cp target/*.pom dep/${hadoop_version}/

    mvn versions:set -DnewVersion=${origin_version}
done
