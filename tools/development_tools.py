def object_info(object, spacing=10, collapse=1):
    """Print methods and doc strings.

    Takes module, class, list, dictionary, or string."""
    methodList = [method for method in dir(object) if callable(getattr(object, method))]
    processFunc = collapse and (lambda s: " ".join(s.split())) or (lambda s: s)
    print "\n".join(["%s %s" %
                     (method.ljust(spacing),
                      processFunc(str(getattr(object, method).__doc__)))
                     for method in methodList])

    if __name__ == "__main__":
        print object_info.__doc__


def convert_dict_to_update(dictionary, roots=None, return_dict=None):
    """
    See: http://stackoverflow.com/questions/19002469/update-a-mongoengine-document-using-a-python-dict


      Returns a new dict that can be used in Document.update(**dict),
      this is used for updating MongoEngine documents with dictionary
      serialized from json.
      >>> data = {'email' : 'email@example.com'}
      >>> convert_dict_to_update(data)
      {'set__email': 'email@example.com'}
      :param dictionary: dictionary with update parameters
      :param roots: roots of nested documents - used for recursion
      :param return_dict: used for recursion
      :return: new dict
    """
    if return_dict is None:
        return_dict = {}
    if roots is None:
        roots = []

    for key, value in dictionary.iteritems():
        if isinstance(value, dict):
            roots.append(key)
            convert_dict_to_update(value, roots=roots, return_dict=return_dict)
            roots.remove(key)  # go one level down in the recursion
        else:
            if roots:
                set_key_name = 'set__{roots}__{key}'.format(
                    roots='__'.join(roots), key=key)
            else:
                set_key_name = 'set__{key}'.format(key=key)
            return_dict[set_key_name] = value

    return return_dict
