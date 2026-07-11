FROM alpine:3.19

RUN apk add --no-cache socat jq

COPY run.sh /
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
