for i in {1..4}; do 
  mkdir -p data/oc-jump$i
  scp oc-jump$i:~/benchmarking/go/results-incr*.csv data/oc-jump$i/
done
