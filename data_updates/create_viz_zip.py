# create a zip file containing the midas-viz-data files

import zipfile
import os

directory = 'midas-viz-data'
output_filename = 'Archive.zip'

#delete the output file if it exists
if os.path.exists(output_filename):
    os.remove(output_filename)

cluster_file = "/Volumes/Samsung-Ext/dev/freq_calc2/output/topic_extraction/20241115-213334/meshTerms-pubmedKeywords-paperAbstract/clusters/cluster-info-Topics_NMF.json"
people_file = "/Volumes/Samsung-Ext/dev/freq_calc2/output/topic_extraction/20241115-213334/meshTerms-pubmedKeywords-paperAbstract/clusters/people-with-clusters.json"

with zipfile.ZipFile(output_filename, 'w') as zipf:
    for root, dirs, files in os.walk(directory):
        for file in files:
            #don't include the path to the file
            zipf.write(os.path.join(root, file), file)
        #add the cluter file and people file
    zipf.write(cluster_file, "cluster-info-Topics_NMF.json")
    zipf.write(people_file, "people-with-clusters.json")

# delete the zip file at ../../midas-visualization/data/webservice/Archive.zip

if os.path.exists("../../midas-visualization/data/webservice/Archive.zip"):
    os.remove("../midas-visualization/data/webservice/Archive.zip")

# move the zip file to ../../midas-visualization/data/webservice
os.rename(output_filename, "../midas-visualization/data/webservice/Archive.zip")
