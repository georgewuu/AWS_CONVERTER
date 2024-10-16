from IPython.display import (
    display,
    display_javascript,
    Javascript,
    IFrame,
    clear_output,
)
import getpass
import base64
import io
import scrapbook as sb
import ast


def show_extension(
    extension, width=300, height=300, url_formula="https://jupyter.soterawireless.io"
):
    user = getpass.getuser()
    url = "{url_formula}/user/{user}/sotera/{extension}/".format(
        url_formula=url_formula, user=user, extension=extension
    )
    handle = display(display_id=True)
    display(IFrame(url, width, height))
    return handle


def jl_fig_set_size(fig, width="10in", height="10in"):
    fig.canvas.layout.width = width
    fig.canvas.layout.height = height


def download_fig(fig, filename="figure.png"):
    with io.BytesIO() as buffer:
        fig.savefig(fname=buffer)
        buffer.seek(0)
        url = "data:image/png;base64,{}".format(
            base64.b64encode(buffer.read()).decode("utf-8")
        )
        jso = Javascript(
            """
         var element = document.createElement('a');
         element.setAttribute('href', '{url}');
         element.setAttribute('download', '{filename}');
         element.style.display = 'none';
         document.body.appendChild(element);
         element.click();
         document.body.removeChild(element);
         """.format(
                url=url, filename=filename
            )
        )
        display_javascript(jso)
    clear_output()


def find_in_notebook(name, variable, type_hint=None):
    nb = sb.read_notebook(name)
    notebook = nb.cells
    idx = str(notebook).find("'text': " + "'" + str(variable) + "=")
    c1 = str(notebook)[idx : idx + 90].split("\\n")[0]
    c = c1.split("=")[1].strip()
    if type_hint == "set":
        print(variable, "is a set")
        c = set(c)
        return f"{variable} = {c}"

    if ("{" in c) or type_hint == "dictionary":
        # print(variable,'is a dict')
        c = ast.literal_eval(c)
        return f"{variable} = {c}"

    elif "[" in c or type_hint == "list":
        # print(variable,'is a list')
        c = ast.literal_eval(c)
        return f"{variable} = {c}"

    elif isinstance(int(c), int) or type_hint == "number":
        return f"{variable} = {c}"

    else:
        c = str(c)
        # print(variable,'is a string',str(c))
    return f"{variable} = {c}"
