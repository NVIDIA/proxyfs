To glide-ify a project (you should not need to do this again for ProxyFS):

    glide init
    glide install

To add a new dependency on github.com/stretchr/testify/assert:

    glide get github.com/stretchr/testify/assert

To "pin" the version of github.com/stretchr/testify/assert to what was just
placed in glide.lock, simply copy the "Version: xxx" line from just under the
github.com/stretchr/testify/assert package line in glide.lock to just under
the github.com/stretchr/testify/assert package line just added to glide.yaml.
