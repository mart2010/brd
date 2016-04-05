# -*- coding: utf-8 -*-

import os

from scrapy.http import Response, Request, TextResponse, HtmlResponse


def mock_from_file(file_name, url=None, response_type="Response", meta=None):
    """
    Create a Scrapy fake HTTP response from a HTML file
    @param file_name: The relative filename from the responses directory,
                      but absolute paths also accepted.
    @param url: The URL of the response.
    returns: A scrapy HTTP response which can be used for unittesting.
    """
    if not url:
        url = 'http://www.example.com'

    request = Request(url=url, meta=meta)
    if not file_name[0] == '/':
        responses_dir = os.path.dirname(os.path.realpath(__file__))
        file_path = os.path.join(responses_dir, file_name)
    else:
        file_path = file_name

    file_content = open(file_path, 'r').read()

    if response_type == "Response":
        response = Response(url=url, request=request, body=file_content)
    elif response_type == "Text":
        response = TextResponse(url=url, request=request, body=file_content, encoding='utf-8')
    elif response_type == "Html":
        response = HtmlResponse(url=url, request=request, body=file_content)
    else:
        raise Exception("None supported response type mocking")

    return response
