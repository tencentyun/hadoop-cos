#!/bin/sh

#=============================================================
# 自动化编译、打包、部署脚本
#
# 对每个支持的 Hadoop 版本依次执行 Maven 部署:
#   mvn clean deploy
# (deploy 会自动累积执行 compile/package/verify/install 等前置阶段)
#
# 部署策略说明:
#   本脚本不在命令行中显式指定部署仓库地址，而是依赖 pom.xml 中
#   默认配置的仓库（central-publishing-maven-plugin，即 Maven 中央仓库）。
#
#   如需部署到腾讯内部 Maven 仓库，请先注释掉 pom.xml 中的
#   central-publishing-maven-plugin 插件配置:
#
#       <plugin>
#           <groupId>org.sonatype.central</groupId>
#           <artifactId>central-publishing-maven-plugin</artifactId>
#           <version>0.8.0</version>
#           <extensions>true</extensions>
#           <configuration>
#               <publishingServerId>central</publishingServerId>
#               <checksums>required</checksums>
#           </configuration>
#       </plugin>
#
#   然后通过 -DaltDeploymentRepository 参数指定内部仓库地址，例如:
#
#       mvn clean deploy -DskipTests \
#           -DaltDeploymentRepository=XXX::default::https://mirrors.tencent.com/repository/maven/XXX
#
# 当 Hadoop 版本 >= 3.3.0 时，自动添加 -Phadoop-3-3 参数激活对应 profile。
#=============================================================

base_dir=$(cd `dirname $0`;pwd)
cd ${base_dir}

hadoop_version_array=("2.8.5" "2.9.2" "3.1.0" "3.2.2" "3.3.0" "3.4.0")

origin_artifact_version=$(mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive exec:exec)

# 备份原始 pom.xml，用于结束时恢复
# 注意：不能使用 git checkout，否则会丢失 pom.xml 中未提交的手动修改
pom_backup="${base_dir}/.pom.xml.bak"
cp pom.xml "${pom_backup}"

#-------------------------------------------------------------
# 判断 Hadoop 版本是否 >= 3.3.0，是则返回 0 (true)
#-------------------------------------------------------------
need_hadoop_3_3_profile() {
    version=$1
    major=$(echo "$version" | cut -d. -f1)
    minor=$(echo "$version" | cut -d. -f2)
    if [ "$major" -gt 3 ] || { [ "$major" -eq 3 ] && [ "$minor" -ge 3 ]; }; then
        return 0
    else
        return 1
    fi
}

#-------------------------------------------------------------
# 恢复 pom.xml 到原始状态（从备份恢复，保留手动修改）
#-------------------------------------------------------------
restore_pom() {
    cd ${base_dir}
    if [ -f "${pom_backup}" ]; then
        cp "${pom_backup}" pom.xml
        rm -f "${pom_backup}"
    fi
    rm -f pom.xml.versionsBackup
}

# 捕获退出信号，确保 pom.xml 被恢复
trap restore_pom EXIT INT TERM

for hadoop_version in ${hadoop_version_array[@]}
do
    echo "======================================================"
    echo "Processing Hadoop version: ${hadoop_version}"
    echo "======================================================"

    # 修改 pom.xml 中的 hadoop.version
    sed -i -E "s/<hadoop\.version>.*<\/hadoop\.version>/<hadoop\.version>${hadoop_version}<\/hadoop\.version>/g" pom.xml

    # 设置构件版本号
    mvn versions:set -DnewVersion=${hadoop_version}-${origin_artifact_version} -DgenerateBackupPoms=false

    # 判断是否需要激活 hadoop-3-3 profile
    profile_args=""
    if need_hadoop_3_3_profile "$hadoop_version"; then
        profile_args="-Phadoop-3-3"
        echo ">> Hadoop ${hadoop_version} >= 3.3.0, enabling hadoop-3-3 profile"
    else
        echo ">> Hadoop ${hadoop_version} < 3.3.0, hadoop-3-3 profile not needed"
    fi

    # 执行 Maven 部署。
    # 注意：Maven 生命周期是累积的，deploy 会自动依次执行 compile、package、
    # verify(含 gpg 签名)、install 等前置阶段。因此这里只需 `clean deploy`，
    # 切勿显式堆叠 `compile package install deploy`——那会导致 gpg:sign 对
    # 已签名的 .asc 文件再次签名，产生非法的 .asc.asc 文件而被仓库拒绝。
    # 任意一步失败则立即中断，不再继续后续版本。
    mvn clean deploy -DskipTests ${profile_args}
    if [ $? -ne 0 ]; then
        echo "Error: Build/deploy failed for Hadoop ${hadoop_version}, aborting."
        exit 1
    fi

    echo ""
done

# 恢复 pom.xml 到原始状态
restore_pom

echo "All done."
