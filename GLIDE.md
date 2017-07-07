To glide-ify a project (you should not need to do this again for ProxyFS):

    glide init
    glide install --strip-vcs

to add a new dependency on stretchr/testify/assert:

    glide get github.com/stretchr/testify/assert --strip-vcs

(this might be required in the case of missed nested dependencies;
glide isn't perfect)

*please* do not forget your `--strip-vcs` flags! you need them to actually
vendorize code!

