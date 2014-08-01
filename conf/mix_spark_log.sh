#!/bin/bash

#sourceDir="/home/ocmp/trunk";
serverDir="/data1/mix_spark/spark_log";
moduleName=(merge bid click expose arrive);
coreNum=(4 4 3 3 2);
executorsNum=(2 2 2 1 1);
#codeModule=(bit );
new=${2}
log_info()
{
   date_str=`date +'%Y-%m-%d %H:%M:%S'`;
   echo "[ $date_str ] ${1}"
}

start_svr()
{
i=0
log_info  "start server now ...";
cd ${serverDir}


  if [ ! -d ${serverDir}/logs ]
      then
          mkdir -p ${serverDir}/logs;
  fi;


   if  [ "${new}" == "all" ] ;
    then
	log_info "start all server..."
       for md_name in "${moduleName[@]}"
         do
	rm ${serverDir}/logs/${md_name}.out*
	log_info "       "
	log_info "delete the ${md_name} log..."	
       nohup  ./bin/spark-submit --executor-cores ${coreNum[$i]} --num-executors ${executorsNum[$i]} --class com.asiainfo.mix.streaming_log.LogAnalysisApp ./lib/streaming-log-0.0.1-SNAPSHOT-jar-with-dependencies.jar ${md_name} > ${serverDir}/logs/${md_name}.out 2>&1 &
	log_info "           "
	log_info "starting ${md_name}, logging to ${serverDir}/logs/${md_name}.out"
	log_info "           "
        sleep 2
        let i++
         done;
         else
	rm ${serverDir}/logs/${new}.out*
	log_info "       "
	log_info "delete the ${new} log..."
        nohup ./bin/spark-submit --class com.asiainfo.mix.streaming_log.LogAnalysisApp ./lib/streaming-log-0.0.1-SNAPSHOT-jar-with-dependencies.jar ${new} > ${serverDir}/logs/${new}.out 2>&1 &
        log_info "           "
        log_info "starting ${new}, logging to ${serverDir}/logs/${new}.out"
        log_info "           "
   fi;
}


stop_svr()
{
 log_info  "stop server now ...";
   if  [ "${new}" == "all" ];
    then
       log_info "stop all server..."
       for md_name in "${moduleName[@]}"
         do
       jps -m  | grep ${md_name} |grep SparkSubmit|  awk '{print $1}'|xargs kill -9
        sleep 2
        log_info "stop ${md_name} done..."
         done;
         else
        jps -m  | grep ${new} | grep SparkSubmit| awk '{print $1}'|xargs kill -9
       log_info "stop ${new} done..."
   fi;
}

check_svr()
{
 jps -m  | grep "SparkSubmit";

}






echo "# ================================================ #";
echo "#                                                  #";
echo "#        Mix log spark services startting!         #";
echo "#                                                  #";
echo "# ================================================ #";

echo "JAVA VERSION : ";
java -version;

sct_name=`basename $0`;

if [ ${#} -lt 1 ] 
   then
      echo "Usage ${sct_name} option [module name]";
      echo "  options : ";
      echo "      stop     stop  server";
      echo "      start    start server";
      echo "      check    check server.";
      echo "  valid module names :";
      echo "   example :";
      echo "     1. start pit";
      echo "        ./${sct_name} start pit";
      echo "     2. start all server";
      echo "        ./${sct_name} start all";
else
   case "${1}" in 
      stop)
         stop_svr;;
      start)
         start_svr;;
      check)
         check_svr;;
      *)
         echo "${1} is not a valid option here!"
         exit 1;;
   esac;
fi;

