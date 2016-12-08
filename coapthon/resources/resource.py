from coapthon import defines


class Resource(object):
    """
    The Resource class.
    """
    def __init__(self, coap_server=None, visible=True, observable=True, allow_children=True):
        """
        Initialize a new Resource.

        :param visible: if the resource is visible
        :param observable: if the resource is observable
        :param allow_children: if the resource could has children
        """
        # The attributes of this resource.
        self.attributes = {}

        # The resource path.
        self.path = None

        # Indicates whether this resource is visible to clients.
        self.visible = visible

        # Indicates whether this resource is observable by clients.
        self.observable = observable

        self.allow_children = allow_children

        self.observe_count = 1

        self._payload = {}

        self._content_type = None

        self.etag = None

        self.location_query = []

        self.max_age = None

        self._coap_server = coap_server

        self.deleted = False
        self.changed = False

    @property
    def payload(self):
        """
        Get the payload of the resource according to the content type specified by required_content_type or
        "text/plain" by default.

        :return: the payload.
        """
        if self._content_type is not None:
            try:
                return self._payload[self._content_type]
            except KeyError:
                raise KeyError("Content-Type not available")
        else:

            if defines.Content_types["text/plain"] in self._payload:
                return self._payload[defines.Content_types["text/plain"]]
            else:
                val = list(self._payload.keys())
                return val[0], self._payload[val[0]]

    @payload.setter
    def payload(self, p):
        """
        Set the payload of the resource.

        :param p: the new payload
        """
        if isinstance(p, tuple):
            k = p[0]
            v = p[1]
            self.actual_content_type = k
            self._payload[k] = v
        else:
            self._payload = {defines.Content_types["text/plain"]: p}

    @property
    def actual_content_type(self):
        """
        Get the actual required Content-Type.

        :return: the actual required Content-Type.
        """
        return self._content_type

    @actual_content_type.setter
    def actual_content_type(self, act):
        """
        Set the actual required Content-Type.

        :param act: the actual required Content-Type.
        """
        self._content_type = act

    @property
    def content_type(self):
        """
        Get the CoRE Link Format ct attribute of the resource.

        :return: the CoRE Link Format ct attribute
        """
        value = ""
        lst = self.attributes.get("ct")
        if lst is not None and len(lst) > 0:
            value = "ct="
            for v in lst:
                value += str(v) + " "
        if len(value) > 0:
            value = value[:-1]
        return value

    @content_type.setter
    def content_type(self, lst):
        """
        Set the CoRE Link Format ct attribute of the resource.

        :param lst: the list of CoRE Link Format ct attribute of the resource
        """
        value = []
        if isinstance(lst, str):
            ct = defines.Content_types[lst]
            value.append(ct)
        elif isinstance(lst, list):
            for ct in lst:
                self.add_content_type(ct)

    def add_content_type(self, ct):
        """
        Add a CoRE Link Format ct attribute to the resource.

        :param ct: the CoRE Link Format ct attribute
        """
        lst = self.attributes.get("ct")
        if lst is None:
            lst = []
        if isinstance(ct, str):
            ct = defines.Content_types[ct]
        lst.append(ct)
        self.attributes["ct"] = lst

    @property
    def resource_type(self):
        """
        Get the CoRE Link Format rt attribute of the resource.

        :return: the CoRE Link Format rt attribute
        """
        value = "rt="
        lst = self.attributes.get("rt")
        if lst is None:
            value = ""
        else:
            value += "\"" + str(lst) + "\""
        return value

    @resource_type.setter
    def resource_type(self, rt):
        """
        Set the CoRE Link Format rt attribute of the resource.

        :param rt: the CoRE Link Format rt attribute
        """
        if not isinstance(rt, str):
            rt = str(rt)
        self.attributes["rt"] = rt

    @property
    def interface_type(self):
        """
        Get the CoRE Link Format if attribute of the resource.

        :return: the CoRE Link Format if attribute
        """
        value = "if="
        lst = self.attributes.get("if")
        if lst is None:
            value = ""
        else:
            value += "\"" + str(lst) + "\""
        return value

    @interface_type.setter
    def interface_type(self, ift):
        """
        Set the CoRE Link Format if attribute of the resource.

        :param ift: the CoRE Link Format if attribute
        """
        if not isinstance(ift, str):
            ift = str(ift)
        self.attributes["if"] = ift

    @property
    def maximum_size_estimated(self):
        """
        Get the CoRE Link Format sz attribute of the resource.

        :return: the CoRE Link Format sz attribute
        """
        value = "sz="
        lst = self.attributes.get("sz")
        if lst is None:
            value = ""
        else:
            value += "\"" + str(lst) + "\""
        return value

    @maximum_size_estimated.setter
    def maximum_size_estimated(self, sz):
        """
        Set the CoRE Link Format sz attribute of the resource.

        :param sz: the CoRE Link Format sz attribute
        """
        if not isinstance(sz, str):
            sz = str(sz)
        self.attributes["sz"] = sz

    def init_resource(self, request, res):
        res.location_query = request.uri_query
        res.payload = (request.content_type, request.payload)
        return res

    def edit_resource(self, request):
        self.location_query = request.uri_query
        self.payload = (request.content_type, request.payload)

    def render_GET(self, request):
        """
        Method to be redefined to render a GET request on the resource.

        :param request: the request
        :return: the response
        """
        raise NotImplementedError

    def render_PUT(self, request):
        """
        Method to be redefined to render a PUTT request on the resource.

        :param request: the request
        :return: the response
        """
        raise NotImplementedError

    def render_POST(self, request):
        """
        Method to be redefined to render a POST request on the resource.

        :param request: the request
        :return: the response
        """
        raise NotImplementedError

    def render_DELETE(self, request):
        """
        Method to be redefined to render a DELETE request on the resource.

        :param request: the request
        """
        raise NotImplementedError



