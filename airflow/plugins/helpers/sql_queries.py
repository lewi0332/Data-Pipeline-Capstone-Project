class SqlQueries:
    get_profile_history = ("""
        INSERT INTO history(
            id,
            followers,
            impressions,
            reach,
            doc_count,
            fol_avg,
            eng_avg,
            colors
        )
            SELECT
                su.id, 
                listagg(su.followers_count, ', ') within group (order by created_at) as followers, 
                listagg(su.impressions, ', ') within group(order by created_at) as impressions,
                listagg(su.reach, ', ') within group (order by created_at) as reach, 
                sa.doc_count, 
                sa.fol_avg, 
                sa.eng_avg,
                sc.colors
            FROM
                staging_users as su 
            LEFT JOIN
                staging_aggregations as sa ON su.id = sa.id
            LEFT JOIN 
                staging_color as sc on su.id = sc.igId
            GROUP
                BY 1,5,6,7,8  ORDER BY 1;
    """)
