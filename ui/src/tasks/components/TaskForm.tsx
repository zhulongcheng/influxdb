// Libraries
import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {
  ComponentSpacer,
  Form,
  Grid,
  Columns,
  Input,
  Radio,
  ButtonShape,
  Button,
  ComponentColor,
  ButtonType,
} from 'src/clockface'
import TaskOptionsOrgDropdown from 'src/tasks/components/TasksOptionsOrgDropdown'
import TaskScheduleFormField from 'src/tasks/components/TaskScheduleFormField'

// Types
import {TaskOptions, TaskSchedule} from 'src/utils/taskOptionsToFluxScript'
import {Alignment, Stack, ComponentStatus} from 'src/clockface/types'
import {Organization} from 'src/api'

// Styles
import './TaskForm.scss'

interface Props {
  orgs: Organization[]
  taskOptions: TaskOptions
  onChangeScheduleType: (schedule: TaskSchedule) => void
  onChangeInput: (e: ChangeEvent<HTMLInputElement>) => void
  onChangeTaskOrgID: (orgID: string) => void
  isInOverlay?: boolean
  onSubmit?: () => void
  canSubmit?: boolean
  dismiss?: () => void
}

interface State {
  retryAttempts: string
  schedule: TaskSchedule
}

export default class TaskForm extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    isInOverlay: false,
    onSubmit: () => {},
    canSubmit: true,
    dismiss: () => {},
  }
  constructor(props) {
    super(props)

    this.state = {
      retryAttempts: '1',
      schedule: props.taskOptions.taskScheduleType,
    }
  }

  public render() {
    const {
      onChangeInput,
      onChangeTaskOrgID,
      taskOptions: {name, taskScheduleType, interval, offset, cron, orgID},
      orgs,
      onSubmit,
      isInOverlay,
    } = this.props

    return (
      <Form onSubmit={onSubmit}>
        <Grid>
          <Grid.Row>
            <Grid.Column widthXS={Columns.Twelve}>
              <Form.Element label="Name">
                <Input
                  name="name"
                  placeholder="Name your task"
                  onChange={onChangeInput}
                  value={name}
                />
              </Form.Element>
            </Grid.Column>
            <Grid.Column widthXS={Columns.Twelve}>
              <Form.Element label="Owner">
                <TaskOptionsOrgDropdown
                  orgs={orgs}
                  selectedOrgID={orgID}
                  onChangeTaskOrgID={onChangeTaskOrgID}
                />
              </Form.Element>
            </Grid.Column>
            <Grid.Column>
              <Form.Element label="Schedule Task">
                <ComponentSpacer
                  align={Alignment.Left}
                  stackChildren={Stack.Rows}
                >
                  <Radio shape={ButtonShape.StretchToFit}>
                    <Radio.Button
                      id="interval"
                      active={taskScheduleType === TaskSchedule.interval}
                      value={TaskSchedule.interval}
                      titleText="Interval"
                      onClick={this.handleChangeScheduleType}
                    >
                      Interval
                    </Radio.Button>
                    <Radio.Button
                      id="cron"
                      active={taskScheduleType === TaskSchedule.cron}
                      value={TaskSchedule.cron}
                      titleText="Cron"
                      onClick={this.handleChangeScheduleType}
                    >
                      Cron
                    </Radio.Button>
                  </Radio>
                </ComponentSpacer>
              </Form.Element>
            </Grid.Column>
            <TaskScheduleFormField
              onChangeInput={onChangeInput}
              schedule={taskScheduleType}
              interval={interval}
              offset={offset}
              cron={cron}
            />
            <Grid.Column widthXS={Columns.Four}>
              <Form.Element label="Retry attempts">
                <Input
                  name="retry"
                  placeholder=""
                  onChange={this.handleChangeRetry}
                  status={ComponentStatus.Disabled}
                  value={this.state.retryAttempts}
                />
              </Form.Element>
            </Grid.Column>
            {isInOverlay && this.buttons}
          </Grid.Row>
        </Grid>
      </Form>
    )
  }

  private get buttons(): JSX.Element {
    const {onSubmit, canSubmit, dismiss} = this.props
    return (
      <Grid.Column widthXS={Columns.Twelve}>
        <Form.Footer>
          <Button
            text="Cancel"
            onClick={dismiss}
            titleText="Cancel save"
            type={ButtonType.Button}
          />
          <Button
            text={'Save as Task'}
            color={ComponentColor.Success}
            type={ButtonType.Submit}
            onClick={onSubmit}
            status={
              canSubmit ? ComponentStatus.Default : ComponentStatus.Disabled
            }
          />
        </Form.Footer>
      </Grid.Column>
    )
  }

  private handleChangeRetry = (e: ChangeEvent<HTMLInputElement>): void => {
    const retryAttempts = e.target.value
    this.setState({retryAttempts})
  }

  private handleChangeScheduleType = (schedule: TaskSchedule): void => {
    this.props.onChangeScheduleType(schedule)
  }
}
