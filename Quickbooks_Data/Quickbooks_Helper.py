def overall_recursion_pl(dict_list_input):
    recursion_results = []
    expected_keys = {'ColData', 'type'}
    def recurse_dict_list(dict_list, header):

        if all(set(d.keys()) == expected_keys for d in dict_list):
            for sub_dict in dict_list:
                refined_sub_dict = sub_dict['ColData']
                result = [v for d in refined_sub_dict for v in d.values()]
                result.append(header)
                recursion_results.append(result)
            return

        for d in dict_list:
            if 'Rows' in d: #I dont think this is the problem
                one_deep_d = d['Rows']
                if one_deep_d == {}:
                    continue
                else:
                    another_list_of_dicts = one_deep_d['Row']
                    # print("//////////")
                    # print(d)
                    if 'Header' in d:
                        recurse_dict_list(another_list_of_dicts, d['Header']['ColData'][0]['value'])
                    else: recurse_dict_list(another_list_of_dicts, header)
            # else: print(d)


    recurse_dict_list(dict_list_input, "")
    return recursion_results