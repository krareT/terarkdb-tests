rm temp_update -rf
mkdir temp_update
cd temp_update
wget nark.cc/download/terark-db-Linux-x86_64-g++-5.3-bmi2-1.tgz 
wget nark.cc/download/terark-fsa_all-Linux-x86_64-g++-5.3-bmi2-1.tgz
tar zxvf ./terark-db-Linux-x86_64-g++-5.3-bmi2-1.tgz
tar zxvf ./terark-fsa_all-Linux-x86_64-g++-5.3-bmi2-1.tgz
cp ./pkg/terark-fsa_all-Linux-x86_64-g++-5.3-bmi2-1/* ./pkg/terark-db-Linux-x86_64-g++-5.3-bmi2-1/ -r
rm ./pkg/terark-fsa_all-Linux-x86_64-g++-5.3-bmi2-1 -rf
rm ~/Documents/pkg -rf
cp ./pkg ~/Documents/ -r
cd ../
