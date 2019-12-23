#Execute from CLI
spark-submit --master local[*] --class org.brig.JsonReader <workdir>/json_reader/target/scala-2.11/json_reader-assembly-0.0.1.jar <path_to>/winemag-data-130k-v2.json
