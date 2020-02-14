import glob
import os
import update.helpers as helpers


def get_annos(full_directory_path):
    annos = []
    for file in glob.glob(full_directory_path + "**/*.xml", recursive=True):
        new_path = os.path.join(full_directory_path, file)
        annos.append(new_path)

    return annos


'''
    This function assumes that your directory structure is as follows:
    -YEAR/WEEK/ANNOS
    -2019/4-10/anno_1.jpeg.xml
    -in_full_directory_path=2019/
'''


def annos_to_csv(in_full_directory_path, s3_bucket_source_name):
    print("in_full_directory_path:", in_full_directory_path)
    annos = get_annos(in_full_directory_path)
    path_split = in_full_directory_path.split('/')
    op_year = path_split[-2]
    print("path_split:", path_split)
    print("op_year:", op_year)
    print("Found: ", len(annos), "annos.")

    ALL_ANNOS = '/io/annos.csv'
    ALL_LBLS = '/io/lbls.csv'

    for counter, anno_full_file_name in enumerate(annos):
        og_filename = str(anno_full_file_name.split('/')[-1])
        meta_data = helpers.get_meta_data(og_filename)

        with open(anno_full_file_name, 'r') as f:
            xml_data = f.read()

            split_anno_name = anno_full_file_name.split("/")
            # print("split_anno_name: ", split_anno_name)
            adjsuted_new_file_name = os.path.join(
                s3_bucket_source_name,
                split_anno_name[-3],
                split_anno_name[-2],
                split_anno_name[-1])
            # print("adjsuted_new_file_name: ", adjsuted_new_file_name)

            anno, lbls = helpers.xml_to_anno_lbls(
                meta_data, xml_data, adjsuted_new_file_name, op_year)

            # print("anno:", anno)
            # print("lbls:", lbls)

            helpers.anno_lbl_to_csv(ALL_ANNOS, anno, lbls, ALL_LBLS,
                                    append=True, append_count=counter)

            if counter % 500 == 0:
                print("Counter: ", counter)
