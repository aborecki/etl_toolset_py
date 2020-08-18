import lxml.etree as ET



def fill_xml_template(xml_template_file, output_xml_file, data):
    tree = ET.parse(xml_template_file)

    ouput_tree=fill_xml_template(tree, data)

    ouput_tree.write(output_xml_file, pretty_print=True,
               xml_declaration=True)

def fill_xml_template_for_loop(xml_tree_template, data):
    while True:
        template = xml_tree_template.find(".//TEMPLATE_FOR_LOOP")
        template_container = template.getparent()

        replace_template_for_loop
        if template is None or len(template) == 0:
            break
        print()

def replace_template_for_loop(template_container, template, data_dict):
    temp_index = template_container.index(template)
    template_name=template.attrib["NAME"]

    if template_name not in data_dict:
        raise Exception("No data for template "+template_name+" found in data_dict")
    template_data = data_dict[template_name]

    for d in template_data:
        #replace template tag by its children
        for ch in template.getchildren():
            template_container.insert(temp_index, ch)
            temp_index+=1

    template_container.remove(template)