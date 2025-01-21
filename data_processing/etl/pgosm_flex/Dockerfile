FROM rustprooflabs/pgosm-flex:1.1.0

COPY ./custom-layerset /custom-layerset

EXPOSE 5433

CMD ["sh", "-c", "python3 docker/pgosm_flex.py \
    --layerset=$LAYERSET \
    --layerset-path=/custom-layerset/ \
    --ram=$RAM \
    --region=$REGION \
    --subregion=$SUBREGION \
    --srid=$SRID \
    --replication"]