#! /bin/bash
echo "starting the service ..."
python main.py --api $OWM_API --location $LOCATION --project $PROJ_NAME --topic $PROJ_TOPIC 

# wait forever not to exit the container
#while true
#do
#  tail -f /dev/null & wait ${!}
#done

exit 0