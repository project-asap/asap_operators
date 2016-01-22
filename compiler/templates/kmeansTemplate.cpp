    // K-means
    get_time (begin);
    auto kmeans_op = asap::kmeans( data_set, num_clusters, max_iters );
    get_time (end);        
    print_time("kmeans", begin, end);

    // Unscale data
    get_time (begin);
    asap::denormalize( extrema, data_set );
    get_time (end);        
    print_time("denormalize", begin, end);

