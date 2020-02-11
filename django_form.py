class ContactPlugin(CMSPluginBase):
    """Enables latest event to be rendered in CMS"""

    model = CMSPlugin
    name = "Form"
    render_template = "forms/forms.html"

    def render(self, context, instance, placeholder):
        request = context['request']
        context.update({
            'instance': instance,
            'placeholder': placeholder,
            'form': ContactForm(request=request),
        })
        return context