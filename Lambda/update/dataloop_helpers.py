import dtlpy
import xmltodict


def xml_to_result(xml_file):
    result = []
    print("\tDoing anno file:", xml_file)

    with open(xml_file) as f:
        xml_dict = xmltodict.parse(f.read())

    image_name = xml_dict['annotation']['filename']

    xml_dict_copy = xml_dict.copy()

    objs = xml_dict['annotation']['object']

    if type(objs) != list:
        objs = [objs]

    for obj in objs:
        if obj.get('polygon', None) is not None and obj.get('bndbox', None) is None:
            print("\tSkipping polygon in file:", xml_file)
            continue
        new_dict = {'topleft': {}, 'bottomright': {}}
        new_dict['label'] = obj['name']
        new_dict['topleft']['x'] = int(float(obj['bndbox']['xmin']))
        new_dict['topleft']['y'] = int(float(obj['bndbox']['ymin']))
        new_dict['bottomright']['x'] = int(float(obj['bndbox']['xmax']))
        new_dict['bottomright']['y'] = int(float(obj['bndbox']['ymax']))
        result.append(new_dict)

    return result, image_name, xml_dict_copy


def login():
    # login
    dtlpy.login_secret(
        email="karthik.paga@ravenind.com",
        password="Raven@2019",
        client_id="Bvv9ajx1GT2VGdvrztD0nNA3SJATKt54",
        client_secret="yrbRGMdhlYwlNxCfiKfzxzNW_q3be4053MsbrkyjFQxK3ryZ1nFt5m9_mnfZ0PvT"
    )


def get_project_dataset(project_name, dataset_name):
    # Get project and dataset
    project = dtlpy.projects.get(project_name=project_name)
    dataset = project.datasets.get(dataset_name=dataset_name)

    return project, dataset


def xml_to_dataloop_item(temp_full_anno_path, project_name, dataset_name):
    login()
    project, dataset = get_project_dataset(project_name, dataset_name)
    annotations, image_name, xml_dict_copy = xml_to_result(temp_full_anno_path)

    filter = dtlpy.Filters()
    filter.add(field='metadata.raven-meta.annotation.filename', values=image_name)
    lst = dataset.items.list(filters=filter)
    item = None
    for page in lst:
        for it in page:
            item = it
            break

    if item is None:
        raise Exception(
            "[Error]: Item NOT FOUND :::: List of items with name:",
            image_name,
            " lst:",
            lst)

    xml_dict_copy['annotation'].pop('object', None)
    item.metadata['raven-meta'] = xml_dict_copy
    item.update()
    builder = item.annotations.builder()

    for annotation in annotations:
        print("\tHere:", annotation)
        # line format if 4 points of bbox
        # this is where you need to update according to your annotation format
        label = annotation['label']

        # extract info
        bottom = annotation['bottomright']['y']
        top = annotation['topleft']['y']
        left = annotation['topleft']['x']
        right = annotation['bottomright']['x']

        # add to builder
        annotation_def = dtlpy.Box(
            label=label,
            top=top,
            bottom=bottom,
            left=left,
            right=right)
        builder.add(annotation_definition=annotation_def)

    # upload annotations
    item.annotations.upload(builder)
