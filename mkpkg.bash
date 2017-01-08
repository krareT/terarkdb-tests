#!/usr/bin/bash

set -e
set -x
shopt -s globstar
export LC_ALL=C
export LANG=C

if [ "$1" = -trial ]; then
	MaybeTrial=$1
	echo building trial version
else
	echo building licenced version
fi

if [ -z "${BMI2}" ]; then
	echo env var BMI2 is required
	exit 1
fi

CompilerList="g++-4.7 g++-4.8 g++-4.9 g++-6 g++-6.1 g++-6.2 g++-5.3 g++-5.4 clang++"
#CompilerList="g++-4.8 g++-4.9 g++-6 g++-6.1 g++-6.2 g++-5.3 g++-5.4 clang++"
#CompilerList="g++-6.2 g++-5.3 g++-5.4 clang++"
#CompilerList="g++-6.1 g++-5.3 clang++"
#CompilerList="g++-5.3 clang++"
#CompilerList="g++-4.8"
#CompilerList="g++-4.9"
CompilerList="g++-4.9 g++-5.4"
for CXX in $CompilerList
do
	export CXX
	if which $CXX; then
		export PATH=/opt/${CXX}/bin:/bin
		COMPILER=`${CXX} terark-tools/detect-compiler.cpp -o a && ./a && rm -f a a.exe`
		UNAME_MachineSystem=`uname -m -s | sed 's:[ /]:-:g'`
		Suffix=${UNAME_MachineSystem}-${COMPILER}-bmi2-${BMI2}
		rm -rf pkg/terarkdb-tests-${Suffix}
		mkdir -p pkg/terarkdb-tests-${Suffix}/{bin,lib,include,tools,shell}
		cp -fr ../terark/pkg/terark-fsa_all-${Suffix}/bin               pkg/terarkdb-tests-${Suffix}/tools
		cp -fr ../terark-db/pkg/terark-db-${Suffix}/lib                 pkg/terarkdb-tests-${Suffix}
		cp -fr ../terark-db/pkg/terark-db-${Suffix}/bin                 pkg/terarkdb-tests-${Suffix}/tools
		cp -fr ../terark-db/pkg/terark-db-${Suffix}/include/terark      pkg/terarkdb-tests-${Suffix}/include
		cp -fr ../terark-zip-rocksdb/pkg/terark-zip-rocksdb-${MaybeTrial}${Suffix}/* pkg/terarkdb-tests-${Suffix}
		cp -fa /opt/$COMPILER/lib64/librocksdb.so*         pkg/terarkdb-tests-${Suffix}/lib
		cp -fa /opt/$COMPILER/lib64/libsnappy*.so*         pkg/terarkdb-tests-${Suffix}/lib
		cp -fa /opt/$COMPILER/lib64/libtbb*.so*            pkg/terarkdb-tests-${Suffix}/lib
		cp -fa /opt/$COMPILER/lib64/libgflags*.so*         pkg/terarkdb-tests-${Suffix}/lib || true # ignore error
		cp -fa /usr/local/mysql/lib/libmysqlclient*.so*    pkg/terarkdb-tests-${Suffix}/lib
		cp -fa /usr/lib64/libzstd.so*                      pkg/terarkdb-tests-${Suffix}/lib
		find * -name CMakeCache.txt | xargs rm -f
		find * -name CMakeFiles     | xargs rm -fr
		export LD_LIBRARY_PATH=/opt/$COMPILER/lib64:$PWD/pkg/terarkdb-tests-${Suffix}/lib
		# brain damanged cmake will ignore CMAKE_VERBOSE_MAKEFILE=ON when CMAKE_CXX_COMPILER is defined
		#-DCMAKE_CXX_COMPILER=`which $CXX`
		cmake -DCMAKE_VERBOSE_MAKEFILE=ON -DCMAKE_SKIP_RPATH=TRUE -DTERARKDB_PATH=$PWD/pkg/terarkdb-tests-${Suffix}
		#sed -i 's:/usr/bin/g++:'$COMPILER:g **/CMakeFiles/**/*.{txt,make}
		make clean
		make -j32
		cp build/Terark_Engine_Test pkg/terarkdb-tests-${Suffix}/bin
		cp shell/Terocksdb_Tpch_*.sh pkg/terarkdb-tests-${Suffix}/shell
		cd pkg
		tar czf terarkdb-tests-${Suffix}.tgz terarkdb-tests-${Suffix}
		cd ..
	else
		echo Not found compiler: $CXX
	fi
done

