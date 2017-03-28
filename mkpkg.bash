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

CompilerList="g++-4.7 g++-4.8 g++-4.9 g++-6 g++-6.1 g++-6.2 g++-5.3 g++-5.4 clang++"
#CompilerList="g++-4.8 g++-4.9 g++-6 g++-6.1 g++-6.2 g++-5.3 g++-5.4 clang++"
#CompilerList="g++-6.2 g++-5.3 g++-5.4 clang++"
#CompilerList="g++-6.1 g++-5.3 clang++"
#CompilerList="g++-5.3 clang++"
CompilerList="g++-4.8"
CompilerList="g++-5.4"
#CompilerList="g++-4.9"
#CompilerList="g++-4.9 g++-5.4"
for BMI2 in 0 1
do
  for CXX in $CompilerList
  do
	export CXX
	export BMI2
	if which $CXX; then
		export PATH=/opt/${CXX}/bin:/bin
		COMPILER=`${CXX} terark-tools/detect-compiler.cpp -o a && ./a && rm -f a a.exe`
		UNAME_MachineSystem=`uname -m -s | sed 's:[ /]:-:g'`
		Suffix=${UNAME_MachineSystem}-${COMPILER}-bmi2-${BMI2}
		rm -rf pkg/terarkdb-tests-${Suffix}
		mkdir -p pkg/terarkdb-tests-${Suffix}/{bin,lib,include,tools,shell}
		cp -fr ../terark/pkg/terark-fsa_all-${Suffix}/bin               pkg/terarkdb-tests-${Suffix}/tools
		cp -fr ../terichdb/pkg/terichdb-${Suffix}/lib                 pkg/terarkdb-tests-${Suffix}
		cp -fr ../terichdb/pkg/terichdb-${Suffix}/bin                 pkg/terarkdb-tests-${Suffix}/tools
		cp -fr ../terichdb/pkg/terichdb-${Suffix}/include/terark      pkg/terarkdb-tests-${Suffix}/include
		cp -fr ../terark-zip-rocksdb/pkg/terark-zip-rocksdb-${MaybeTrial}${Suffix}/include pkg/terarkdb-tests-${Suffix}
		cp -fa ../terark-zip-rocksdb/pkg/terark-zip-rocksdb-${MaybeTrial}${Suffix}/lib     pkg/terarkdb-tests-${Suffix}
		cp -fa ../terark-zip-rocksdb/pkg/terark-zip-rocksdb-${MaybeTrial}${Suffix}/lib_static/libterark-zip-rocksdb*.a pkg/terarkdb-tests-${Suffix}/lib
		cp -fa /opt/$COMPILER/bin/ldb                      pkg/terarkdb-tests-${Suffix}/bin
		cp -fa /opt/$COMPILER/lib64/librocksdb.so*         pkg/terarkdb-tests-${Suffix}/lib
		cp -fa /opt/$COMPILER/lib64/libsnappy*.so*         pkg/terarkdb-tests-${Suffix}/lib
		cp -fa /opt/$COMPILER/lib64/libtbb*.so*            pkg/terarkdb-tests-${Suffix}/lib
		cp -fa /opt/$COMPILER/lib64/libgflags*.so*         pkg/terarkdb-tests-${Suffix}/lib || true # ignore error
		cp -fa /usr/local/mysql/lib/libmysqlclient*.so*    pkg/terarkdb-tests-${Suffix}/lib
		cp -fa /usr/lib64/libzstd.so*                      pkg/terarkdb-tests-${Suffix}/lib
		find * -name CMakeCache.txt | xargs rm -f
		find * -name CMakeFiles     | xargs rm -fr
		export LD_LIBRARY_PATH=/opt/$COMPILER/lib64:$PWD/pkg/terarkdb-tests-${Suffix}/lib
		rm -f  pkg/terarkdb-tests-${Suffix}/lib/libterark-zip-rocksdb*.so*
		# brain damanged cmake will ignore CMAKE_VERBOSE_MAKEFILE=ON when CMAKE_CXX_COMPILER is defined
		#-DCMAKE_CXX_COMPILER=`which $CXX`
		cmake -DCMAKE_VERBOSE_MAKEFILE=ON -DCMAKE_SKIP_RPATH=TRUE -DTERARKDB_PATH=$PWD/pkg/terarkdb-tests-${Suffix}
		#sed -i 's:/usr/bin/g++:'$COMPILER:g **/CMakeFiles/**/*.{txt,make}
		make clean
		make -j32
		rm -f pkg/terarkdb-tests-${Suffix}/lib/libterark-zip-rocksdb*
		cp build/Terark_Engine_Test pkg/terarkdb-tests-${Suffix}/bin
		cp shell/Terarkdb_Tpch_*.sh pkg/terarkdb-tests-${Suffix}/shell
		cd pkg
		tar czf terarkdb-tests-${Suffix}.tgz terarkdb-tests-${Suffix}
		cd ..
	else
		echo Not found compiler: $CXX
	fi
  done
done

