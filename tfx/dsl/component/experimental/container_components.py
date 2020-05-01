# Lint as: python2, python3
# Copyright 2020 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Functions for creating container components."""

# TODO(b/149535307): Remove __future__ imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from typing import List, Text

from tfx.components.base import base_component
from tfx.dsl.component.experimental import executor_specs
from tfx.types import channel
from tfx.types import component_spec


class InputSpec(object):
  """Desscribes a component input."""

  def __init__(
      self,
      name: Text,
      type,  # pylint: disable=redefined-builtin
      optional: bool,
  ):
    self.name = name
    self.type = type
    self.optional = optional


class OutputSpec(object):
  """Desscribes a component input."""

  def __init__(
      self,
      name: Text,
      type,  # pylint: disable=redefined-builtin
  ):
    self.name = name
    self.type = type


def _create_channel_with_empty_artifact(artifact_type) -> channel.Channel:
  return channel.Channel(
      type=artifact_type,
      artifacts=[
          artifact_type(),
      ],
  )


def create_container_component(
    name: Text,
    inputs: List[InputSpec],
    outputs: List[InputSpec],
    parameters: List[InputSpec],
    image: Text,
    command: List[executor_specs.CommandlineArgumentType],
) -> base_component.BaseComponent:
  """Creates a container-based component.

  Args:
    name: The name of the component
    inputs: The list of component inputs
    outputs: The list of component outputs
    parameters: The list of component parameters

    image: Container image name.
    command: Container entrypoint command-line. Not executed within a shell.
      The command-line can use placeholder objects that will be replaced at
      the compilation time. Note: Jinja templates are not supported.

  Returns:
    Component that can be instantiated and user inside pipeline.

  Example:

    component = create_container_component(
        inputs=[
            InputSpec(name='training_data', type=Dataset),
        ],
        outputs=[
            OutputSpec(name='model', type=Model),
        ],
        parameters=[
            InputSpec(name='num_training_steps', type=int),
        ],
        image='gcr.io/my-project/my-trainer',
        command=[
            'python3', 'my_trainer',
            '--training_data_uri', InputUriPlaceholder('training_data'),
            '--model_uri', OutputUriPlaceholder('model'),
            '--num_training-steps', InputValuePlaceholder('num_training_steps'),
        ]
    )
  """

  component_name = name

  input_channel_parameters = {}
  output_channel_parameters = {}
  default_input_channels = {}
  output_channels = {}
  execution_parameters = {}

  for input_spec in inputs or []:
    python_input_name = input_spec.name  # TODO(avolkov) Sanitize

    input_channel_parameters[python_input_name] = (
        component_spec.ChannelParameter(type=input_spec.type))
    if input_spec.optional:
      default_input_channels[python_input_name] = (
          _create_channel_with_empty_artifact(input_spec.type))

  for output_spec in outputs or []:
    python_output_name = output_spec.name  # TODO(avolkov) Sanitize
    output_channel_parameters[python_output_name] = (
        component_spec.ChannelParameter(type=output_spec.type))
    output_channels[python_output_name] = _create_channel_with_empty_artifact(
        output_spec.type)

  for input_spec in parameters or []:
    python_input_name = input_spec.name  # TODO(avolkov) Sanitize

    execution_parameters[python_input_name] = (
        component_spec.ExecutionParameter(type=input_spec.type))

  component_name = component_name or 'Component'
  component_class_name = component_name  # TODO(avolkov) Sanitize

  tfx_component_spec_class = type(
      component_class_name + 'Spec',
      (component_spec.ComponentSpec,),
      dict(
          PARAMETERS=execution_parameters,
          INPUTS=input_channel_parameters,
          OUTPUTS=output_channel_parameters,
          # __doc__=component_class_doc,
      ),
  )

  def tfx_component_class_init(self, **kwargs):
    instance_name = kwargs.pop('instance_name', None)
    arguments = {}
    arguments.update(default_input_channels)
    arguments.update(output_channels)
    arguments.update(kwargs)

    base_component.BaseComponent.__init__(
        self,
        spec=self.__class__.SPEC_CLASS(**arguments),
        instance_name=instance_name,
    )

  tfx_component_class = type(
      component_class_name,
      (base_component.BaseComponent,),
      dict(
          SPEC_CLASS=tfx_component_spec_class,
          EXECUTOR_SPEC=executor_specs.TemplatedExecutorContainerSpec(
              image=image,
              command=command,
          ),
          __init__=tfx_component_class_init,
          # __doc__=component_class_doc,
      ),
  )
  return tfx_component_class
