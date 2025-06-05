#! /bin/sh

# Wait for the pod to get a healthcheck
STATUS=$(docker inspect --format='{{json .State.Health.Status}}' fletcher)
while [ "$STATUS" = '"starting"' ]
do
  echo "pod status: $STATUS - $(date)"
  sleep 1
  STATUS=$(docker inspect --format='{{json .State.Health.Status}}' fletcher)
done

# Is the pod healthy?
echo "pod status: $STATUS - $(date)"
if [ "$STATUS" = '"healthy"' ]
then
  exit 0
else
  exit 1
fi
